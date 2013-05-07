# Copyright 2013 Rackspace
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import ConfigParser
import logging

import cli_tools
import eventlet
import pkg_resources
import redis


LOG = logging.getLogger(__name__)


class SubwayException(Exception):
    """
    An exception class for reporting errors from Subway.
    """

    pass


class SubwayDaemon(object):
    """
    The Subway daemon.  Subway listens for reload commands coming over
    the Turnstile control channel; upon receiving one, it will copy
    the limits configuration from the master to all configured slaves,
    subsequently forwarding the reload command.  It will also forward
    other Turnstile commands.

    Note: At present, replies via the publish/subscribe mechanism do
    not work; this in particular means that the "ping" command does
    not function.
    """

    _commands = {}

    @classmethod
    def _register(cls, name, func):
        """
        Register a function as an implementation of the named command.
        This is used exclusively for internally implemented commands,
        e.g., "reload"; other extension commands can be implemented as
        functions in the "subway.commands" entrypoint group.

        :param name: The name of the command.
        :param func: The implementing function.
        """

        cls._commands[name] = func

    def __init__(self, config, master, slaves):
        """
        Initialize the ``SubwayDaemon``.

        :param config: A dictionary containing essential configuration
                       values, such as "channel" or "limits_key".
        :param master: A ``StrictRedis`` instance configured to
                       connect to the master database.
        :param slaves: A list of ``StrictRedis`` instances, each
                       configured to connect to a different slave
                       database.
        """

        # Save the configuration and the redis instances
        self.config = config
        self.master = master
        self.slaves = slaves

        self.limits = []

        # Need a semaphore to cover reloads in action
        self.pending = eventlet.semaphore.Semaphore()

    def listen(self):
        """
        Subscribe to and listen for messages on the master server's
        control queue.
        """

        # Need a pub-sub object
        kwargs = {}
        if 'shard_hint' in self.config:
            kwargs['shard_hint'] = self.config['shard_hint']
        pubsub = self.master.pubsub(**kwargs)

        # Subscribe to the right channel
        channel = self.config.get('channel', 'control')
        pubsub.subscribe(channel)

        # Now we listen...
        for msg in pubsub.listen():
            # Only interested in messages to our control channel
            if (msg['type'] in ('pmessage', 'message') and
                    msg['channel'] == channel):
                # Figure out what kind of message this is
                command, _sep, args = msg['data'].partition(':')

                # We must have some command...
                if not command:
                    continue

                # Don't do anything with internal commands
                if command[0] == '_':
                    LOG.error("Cannot call internal command %r" % command)
                    continue

                # Look up the implementing function
                func = self.find_command(command)

                # And execute it
                try:
                    func(self, command, args)
                except Exception:
                    LOG.exception("Failed to handle command %r arguments %r" %
                                  (command, args))

    def find_command(self, command):
        """
        Look up the function implementing the given command.

        :param command: The name of the command.

        :returns: A callable implementing the command.  If the command
                  is not implemented either natively nor in the
                  "subway.commands" entrypoint group, the command will
                  be forwarded unchanged to all slaves.
        """

        # If we don't have the command, resolve it via entry points
        if command not in self._commands:
            for ep in pkg_resources.iter_entry_points("subway.commands",
                                                      command):
                try:
                    # Try loading the entry point...
                    func = ep.load()
                except (ImportError, AttributeError,
                        pkg_resources.UnknownExtra):
                    # Failed, see if there are any others of the same
                    # name
                    continue
                else:
                    # We have success!
                    self._commands[command] = func
                    break
            else:
                # We didn't find an implementing function; cache the
                # negative result
                self._commands[command] = None

        # If we didn't find a command, use the generic message
        # forwarder
        return self._commands[command] or forward

    def reload(self, args=None):
        """
        Reload the limits configuration from the master database.  The
        limits configuration will be forwarded to all slave databases,
        and an appropriate "reload" command sent on all slave database
        control queues.

        :param args: If given, provides arguments to include as part
                     of the reload command.
        """

        send_reload = False
        key = self.config.get('limits_key', 'limits')

        # Acquire the pending semaphore.  If we fail, exit--someone
        # else is already doing the reload
        if not self.pending.acquire(False):
            return

        try:
            # Load the limits
            limits = self.master.zrange(key, 0, -1)

            if limits != self.limits:
                self.limits = limits
                send_reload = True
        except Exception:
            # Log an error
            LOG.exception("Could not load limits")
        finally:
            self.pending.release()

        if send_reload:
            # First, need to alter the limits on all the slaves
            for slave in self.slaves:
                self.update_limits(key, slave)

            # Now, forward the reload on to the slave
            forward(self, 'reload', args)

    def update_limits(self, key, slave):
        """
        Perform the actual limits update on a given slave.  The limits
        currently cached in the ``SubwayDaemon`` instance will be sent
        to the given slave.

        :param key: The key for storage of the limits configuration on
                    the slave.
        :param slave: The ``StrictRedis`` instance for the slave.
        """

        limits_set = set(self.limits)

        with slave.pipeline() as pipe:
            while True:
                try:
                    # Watch for changes to the key
                    pipe.watch(key)

                    # Look up the existing limits
                    existing = set(pipe.zrange(key, 0, -1))

                    # Start the transaction...
                    pipe.multi()

                    # Remove limits we no longer have
                    for lim in existing - limits_set:
                        pipe.zrem(key, lim)

                    # Update or add all our desired limits
                    for idx, lim in enumerate(self.limits):
                        pipe.zadd(key, (idx + 1) * 10, lim)

                    # Execute the transaction
                    pipe.execute()
                except redis.WatchError:
                    # Try again...
                    continue
                else:
                    # We're all done!
                    break


def forward(server, command, args):
    """
    Forward a command to all slaves.

    :param server: The ``SubwayDaemon`` instance.
    :param command: The command to forward.
    :param args: Any arguments to provide for the command.  These
                 arguments must already be constructed as a single
                 string; note that Turnstile uses the colon character
                 (":") as an argument separator.
    """

    # Determine the channel to send to
    channel = server.config.get('channel', 'control')

    # Build up the message to send
    message = command
    if args:
        message += ':%s' % args

    # Forward the message to all slaves
    for slave in server.slaves:
        slave.publish(channel, message)


def register(name, func=None):
    """
    Register a function as a command.  This is really only meant for
    internally-implemented commands.

    :param name: The command name.
    :param func: The implementing function.  If not provided, a
                 decorator will be returned.

    :returns: If the ``func`` argument is provided, it is returned;
              otherwise, a callable taking one argument--the
              function--will be returned.  This allows the
              ``register()`` function to be used as a decorator.
    """

    def decorator(func):
        # Perform the registration
        SubwayDaemon._register(name, func)
        return func

    # If func was given, call the decorator, otherwise, return the
    # decorator
    if func:
        return decorator(func)
    else:
        return decorator


@register('reload')
def reload(server, command, args):
    """
    Implement the "reload" command.  Interprets the arguments and
    applies the appropriate load type and spread.  Note that if no
    reload type is provided, but a default spread value is configured,
    the reload will be forwarded as a "spread" with the configured
    spread.

    :param server: The ``SubwayDaemon`` instance.
    :param command: The command to forward.
    :param args: Any arguments to provide for the command.  These
                 arguments must already be constructed as a single
                 string; note that Turnstile uses the colon character
                 (":") as an argument separator.
    """

    # Process the arguments
    arglist = args.split(':')
    load_type = None
    spread = None
    if arglist:
        load_type = arglist.pop(0)
        if load_type == 'immediate':
            # Immediate reload
            pass
        elif load_type == 'spread':
            # Spread within an interval; what interval?
            try:
                spread = float(arglist.pop(0))
            except (TypeError, ValueError):
                # Not given or not a valid float; use the configured
                # value
                load_type = None
        else:
            # Unrecognized load type
            load_type = None

    # Use any configured spread
    if load_type is None:
        try:
            spread = float(server.config['reload_spread'])
        except (TypeError, ValueError, KeyError):
            # No valid configuration
            pass

    if spread:
        # Apply a randomization to spread the load around
        args = "spread:%s" % spread
        eventlet.spawn_after(random.random() * spread, server.reload, args)
    else:
        # Spawn in immediate mode
        eventlet.spawn_n(server.reload, load_type)


REDIS_CONFIGS = {
    'host': str,
    'port': int,
    'db': int,
    'password': str,
    'socket_timeout': int,
    'unix_socket_path': str,
}


def get_database(config):
    """
    Retrieve a ``StrictRedis`` instance based on the given
    configuration.

    :param config: A dictionary of configuration values for the
                   connection to the Redis server.  All values are
                   assumed to be strings.

    :returns: An instance of ``StrictRedis`` configured for the
              designated Redis server.
    """

    # Convert the configuration to keyword arguments for StrictRedis
    kwargs = {}
    for cfg_var, type_ in REDIS_CONFIGS.items():
        if cfg_var in config:
            kwargs[cfg_var] = type_(config[cfg_var])

    # Make sure we have at a minimum the hostname
    if 'host' not in kwargs and 'unix_socket_path' not in kwargs:
        raise redis.ConnectionError("No host specified for redis database")

    # Build and return the database client
    return redis.StrictRedis(**kwargs)


@cli_tools.argument('config',
                    help="Configuration file for subway.")
def subway(config):
    """
    Run the Subway limits configuration synchronization daemon.

    :param config: The configuration file for Subway.
    """

    # Set up eventlet, first thing...
    eventlet.monkey_patch()

    # Read the configuration file
    conf = ConfigParser.SafeConfigParser()
    conf.read([config])

    # Suck in the subway config itself
    try:
        subway_config = dict(conf.items('config'))
    except ConfigParser.NoSectionError:
        # Use the defaults across the board
        subway_config = {}

    # Demand a "master" section
    if not conf.has_section('master'):
        raise SubwayException("Missing required configuration for the master")

    # OK, let's set up the master
    master = get_database(dict(conf.items('master')))

    # Now set up the slaves...
    slaves = []
    for sect in conf.sections:
        if not sect.startswith('slave:'):
            continue

        # OK, we don't actually care about the slave's name, and we'll
        # only log errors trying to set up the connection to the slave
        try:
            slaves.append(get_database(dict(conf.items(sect))))
        except redis.ConnectionError:
            LOG.exception("Failed to connect to slave %r" %
                          sect[len('slave:'):])

    # Make sure we have at least one slave
    if not slaves:
        raise SubwayException("Missing configuration for slaves")

    # Set up the daemon...
    server = SubwayDaemon(subway_config, master, slaves)

    # Do the initial limits loading, as a thread; we use spawn_after()
    # so we make sure the listening thread gets started first, so we
    # don't miss any reload commands
    eventlet.spawn_after(2.0, server.reload)

    # Now, start listening for messages
    server.listen()
