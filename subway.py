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

import logging

import eventlet
import pkg_resources
import redis


LOG = logging.getLogger(__name__)


class SubwayDaemon(object):
    _commands = {}

    @classmethod
    def _register(cls, name, func):
        cls._commands[name] = func

    def __init__(self, config, master, slaves):
        self.config = config
        self.master = master
        self.slaves = slaves

        self.limits = []

        # Need a semaphore to cover reloads in action
        self.pending = eventlet.semaphore.Semaphore()

        # Initialize the listening thread
        self.listen_thread = None

    def start(self):
        # Spawn the listening thread
        self.listen_thread = eventlet.spawn_n(self.listen)

        # And do the initial load
        self.reload()

    def listen(self):
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
