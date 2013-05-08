=============================
Subway Rate Limit Distributor
=============================

Turnstile retrieves its rate limits configuration from a Redis
database.  It also listens on a "control" channel for commands
ordering it to reload that configuration.  There is some capability to
spread this reload across a time interval, but in deployments with
large numbers of Turnstile instances, this could still result in a
large number of queries to a single Redis server.

Subway provides a means to mitigate this problem.  Subway listens to
the "control" channel on a single "master" server; when it receives a
command to reload the limits, it distributes the limits configuration
to a set of slave servers and forwards the reload command to the
"control" channel on those slave servers.  This can allow the limits
to be fanned out to a large number of Redis servers, avoiding the
single-bottleneck described above.

Configuring Subway
==================

Subway requires a single command-line argument, which specifies the
configuration file.  The configuration file is an INI-style file.  The
basic configuration for Subway is in the ``[config]`` section; all
values here are optional, and the recognized options are:

channel
    Specifies the control channel used by Turnstile.  This will be
    used for both the master and all slaves.  It defaults to
    "control".

limits_key
    Specifies the key in which the rate limits are stored.  This will
    be used for both the master and all slaves.  It defaults to
    "limits".

reload_spread
    Normally, when a reload occurs, the limits are reloaded from the
    database immediately.  This configuration option allows the
    declaration of a time interval; the reload will be scheduled at a
    random time within the specified interval, after receipt of a
    reload command.  It is possible for the reload command to override
    this value; see Turnstile's ``setup_limits`` command for more
    information.

shard_hint
    Subway calls the ``pubsub()`` method of ``StrictRedis``, which
    takes an optional ``shard_hint`` argument, which could be used by
    the underlying client for sharding.  This configuration option
    allows this ``shard_hint`` to be specified.

The remaining sections--``[master]`` and all the ``[slave:*]``
sections--specify the connection to the master or slave Redis servers.
The recognized options for these sections are as follows:

db
    The database number on the Redis server to connect to.  Must be an
    integer.  Optional.

host
    The host name of the Redis server to connect to.  Either this or
    ``unix_socket_path`` must be provided.

password
    The required authentication password for the Redis server.
    Optional.

port
    The port number of the Redis server to connect to.  Must be an
    integer.  Optional.

socket_timeout
    Allows specification of an alternate socket timeout, in seconds.
    Must be an integer.  Optional.

unix_socket_path
    The path to a UNIX socket for the Redis server.  Either this or
    ``host`` must be provided.

The ``[master]`` section is required, and specifies the master Redis
server for the limits data.  Slaves are specified using
``[slave:<name>]``, where ``<name>`` may be any string; this string is
not used by Subway and exists solely to allow specification of slaves.
At least one slave must be specified.

Running Subway
==============

Subway requires a single argument, the configuration file described
above.  Following is a usage summary for the ``subway`` daemon::

    usage: subway [-h] config

    Run the Subway limits configuration synchronization daemon.

    positional arguments:
      config      Configuration file for subway.

    optional arguments:
      -h, --help  show this help message and exit
