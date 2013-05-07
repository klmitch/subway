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

import eventlet.semaphore
import mock
import pkg_resources
import redis
import unittest2

import subway


class TestException(Exception):
    pass


class TestSubwayDaemon(unittest2.TestCase):
    @mock.patch.dict(subway.SubwayDaemon._commands, clear=True)
    def test_register(self):
        subway.SubwayDaemon._register('spam', 'spammer')

        self.assertEqual(subway.SubwayDaemon._commands, dict(spam='spammer'))

    def test_registered(self):
        self.assertEqual(subway.SubwayDaemon._commands,
                         dict(reload=subway.reload))

    def test_init(self):
        sd = subway.SubwayDaemon('config', 'master', ['slave1', 'slave2'])

        self.assertEqual(sd.config, 'config')
        self.assertEqual(sd.master, 'master')
        self.assertEqual(sd.slaves, ['slave1', 'slave2'])
        self.assertEqual(sd.limits, [])
        self.assertIsInstance(sd.pending, eventlet.semaphore.Semaphore)

    @mock.patch.object(subway, 'LOG')
    @mock.patch.object(subway.SubwayDaemon, 'find_command',
                       return_value=mock.Mock())
    def test_listen_defaults(self, mock_find_command, mock_LOG):
        config = {}
        pubsub = mock.Mock(**{
            'listen.return_value': [
                # Bad type
                {
                    'type': 'other',
                    'channel': 'control',
                    'data': 'test:message:with:args',
                },
                # Bad channel
                {
                    'type': 'message',
                    'channel': 'other',
                    'data': 'test:message:with:args',
                },
                {
                    'type': 'pmessage',
                    'channel': 'other',
                    'data': 'test:message:with:args',
                },
                # Missing command
                {
                    'type': 'message',
                    'channel': 'control',
                    'data': ':message:with:args',
                },
                {
                    'type': 'pmessage',
                    'channel': 'control',
                    'data': ':message:with:args',
                },
                # Internal command
                {
                    'type': 'message',
                    'channel': 'control',
                    'data': '_test:message:with:args',
                },
                {
                    'type': 'pmessage',
                    'channel': 'control',
                    'data': '_test:message:with:args',
                },
                # Complete message, with args
                {
                    'type': 'message',
                    'channel': 'control',
                    'data': 'test:message:with:args',
                },
                {
                    'type': 'pmessage',
                    'channel': 'control',
                    'data': 'test:message:with:args',
                },
                # Complete message, with no args
                {
                    'type': 'message',
                    'channel': 'control',
                    'data': 'test_no_args',
                },
                {
                    'type': 'pmessage',
                    'channel': 'control',
                    'data': 'test_no_args',
                },
            ],
        })
        master = mock.Mock(**{
            'pubsub.return_value': pubsub,
        })
        sd = subway.SubwayDaemon(config, master, 'slaves')

        sd.listen()

        master.pubsub.assert_called_once_with()
        pubsub.subscribe.assert_called_once_with('control')
        pubsub.listen.assert_called_once_with()
        mock_LOG.assert_has_calls([
            mock.call.error("Cannot call internal command '_test'"),
            mock.call.error("Cannot call internal command '_test'"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 2)
        mock_find_command.assert_has_calls([
            mock.call('test'),
            mock.call('test'),
            mock.call('test_no_args'),
            mock.call('test_no_args'),
        ])
        self.assertEqual(mock_find_command.call_count, 4)
        mock_find_command.return_value.assert_has_calls([
            mock.call(sd, 'test', 'message:with:args'),
            mock.call(sd, 'test', 'message:with:args'),
            mock.call(sd, 'test_no_args', ''),
            mock.call(sd, 'test_no_args', ''),
        ])
        self.assertEqual(mock_find_command.return_value.call_count, 4)

    @mock.patch.object(subway, 'LOG')
    @mock.patch.object(subway.SubwayDaemon, 'find_command',
                       return_value=mock.Mock(side_effect=TestException))
    def test_listen_alternates(self, mock_find_command, mock_LOG):
        config = {
            'shard_hint': 'hint',
            'channel': 'alternate',
        }
        pubsub = mock.Mock(**{
            'listen.return_value': [
                # Bad type
                {
                    'type': 'other',
                    'channel': 'alternate',
                    'data': 'test:message:with:args',
                },
                # Bad channel
                {
                    'type': 'message',
                    'channel': 'other',
                    'data': 'test:message:with:args',
                },
                {
                    'type': 'pmessage',
                    'channel': 'other',
                    'data': 'test:message:with:args',
                },
                # Missing command
                {
                    'type': 'message',
                    'channel': 'alternate',
                    'data': ':message:with:args',
                },
                {
                    'type': 'pmessage',
                    'channel': 'alternate',
                    'data': ':message:with:args',
                },
                # Internal command
                {
                    'type': 'message',
                    'channel': 'alternate',
                    'data': '_test:message:with:args',
                },
                {
                    'type': 'pmessage',
                    'channel': 'alternate',
                    'data': '_test:message:with:args',
                },
                # Complete message, with args
                {
                    'type': 'message',
                    'channel': 'alternate',
                    'data': 'test:message:with:args',
                },
                {
                    'type': 'pmessage',
                    'channel': 'alternate',
                    'data': 'test:message:with:args',
                },
                # Complete message, with no args
                {
                    'type': 'message',
                    'channel': 'alternate',
                    'data': 'test_no_args',
                },
                {
                    'type': 'pmessage',
                    'channel': 'alternate',
                    'data': 'test_no_args',
                },
            ],
        })
        master = mock.Mock(**{
            'pubsub.return_value': pubsub,
        })
        sd = subway.SubwayDaemon(config, master, 'slaves')

        sd.listen()

        master.pubsub.assert_called_once_with(shard_hint='hint')
        pubsub.subscribe.assert_called_once_with('alternate')
        pubsub.listen.assert_called_once_with()
        mock_LOG.assert_has_calls([
            mock.call.error("Cannot call internal command '_test'"),
            mock.call.error("Cannot call internal command '_test'"),
            mock.call.exception("Failed to handle command 'test' "
                                "arguments 'message:with:args'"),
            mock.call.exception("Failed to handle command 'test' "
                                "arguments 'message:with:args'"),
            mock.call.exception("Failed to handle command 'test_no_args' "
                                "arguments ''"),
            mock.call.exception("Failed to handle command 'test_no_args' "
                                "arguments ''"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 6)
        mock_find_command.assert_has_calls([
            mock.call('test'),
            mock.call('test'),
            mock.call('test_no_args'),
            mock.call('test_no_args'),
        ])
        self.assertEqual(mock_find_command.call_count, 4)
        mock_find_command.return_value.assert_has_calls([
            mock.call(sd, 'test', 'message:with:args'),
            mock.call(sd, 'test', 'message:with:args'),
            mock.call(sd, 'test_no_args', ''),
            mock.call(sd, 'test_no_args', ''),
        ])
        self.assertEqual(mock_find_command.return_value.call_count, 4)

    @mock.patch.dict(subway.SubwayDaemon._commands, clear=True, spam='spammer')
    @mock.patch.object(pkg_resources, 'iter_entry_points')
    def test_find_command_declared(self, mock_iter_entry_points):
        sp = subway.SubwayDaemon('config', 'master', 'slaves')

        result = sp.find_command('spam')

        self.assertEqual(result, 'spammer')
        self.assertEqual(sp._commands, dict(spam='spammer'))
        self.assertFalse(mock_iter_entry_points.called)

    @mock.patch.dict(subway.SubwayDaemon._commands, clear=True, spam=None)
    @mock.patch.object(pkg_resources, 'iter_entry_points')
    def test_find_command_miss_cached(self, mock_iter_entry_points):
        sp = subway.SubwayDaemon('config', 'master', 'slaves')

        result = sp.find_command('spam')

        self.assertEqual(result, subway.forward)
        self.assertEqual(sp._commands, dict(spam=None))
        self.assertFalse(mock_iter_entry_points.called)

    @mock.patch.dict(subway.SubwayDaemon._commands, clear=True)
    @mock.patch.object(pkg_resources, 'iter_entry_points',
                       return_value=[])
    def test_find_command_cache_miss(self, mock_iter_entry_points):
        sp = subway.SubwayDaemon('config', 'master', 'slaves')

        result = sp.find_command('spam')

        self.assertEqual(result, subway.forward)
        self.assertEqual(sp._commands, dict(spam=None))
        mock_iter_entry_points.assert_called_once_with(
            'subway.commands', 'spam')

    @mock.patch.dict(subway.SubwayDaemon._commands, clear=True)
    @mock.patch.object(pkg_resources, 'iter_entry_points')
    def test_find_command_lookup(self, mock_iter_entry_points):
        entrypoints = [
            mock.Mock(**{'load.side_effect': ImportError}),
            mock.Mock(**{'load.side_effect': AttributeError}),
            mock.Mock(**{'load.side_effect': pkg_resources.UnknownExtra}),
            mock.Mock(**{'load.return_value': 'spammer'}),
            mock.Mock(**{'load.return_value': 'overrun'}),
        ]
        mock_iter_entry_points.return_value = entrypoints
        sp = subway.SubwayDaemon('config', 'master', 'slaves')

        result = sp.find_command('spam')

        self.assertEqual(result, 'spammer')
        self.assertEqual(sp._commands, dict(spam='spammer'))
        mock_iter_entry_points.assert_called_once_with(
            'subway.commands', 'spam')
        for ep in entrypoints[:-1]:
            ep.load.assert_called_once_with()
        self.assertFalse(entrypoints[-1].load.called)

    @mock.patch('eventlet.semaphore.Semaphore', return_value=mock.Mock(**{
        'acquire.return_value': False,
    }))
    @mock.patch.object(subway, 'LOG')
    @mock.patch.object(subway.SubwayDaemon, 'update_limits')
    @mock.patch.object(subway, 'forward')
    def test_reload_acquire_fail(self, mock_forward, mock_update_limits,
                                 mock_LOG, mock_Semaphore):
        config = {}
        master = mock.Mock(**{'zrange.return_value': []})
        slaves = ['slave1', 'slave2', 'slave3']
        sd = subway.SubwayDaemon(config, master, slaves)

        sd.reload()

        sd.pending.acquire.assert_called_once_with(False)
        self.assertFalse(sd.pending.release.called)
        self.assertFalse(master.zrange.called)
        self.assertFalse(mock_forward.called)
        self.assertFalse(mock_update_limits.called)
        self.assertEqual(len(mock_LOG.method_calls), 0)
        self.assertEqual(sd.limits, [])

    @mock.patch('eventlet.semaphore.Semaphore', return_value=mock.Mock(**{
        'acquire.return_value': True,
    }))
    @mock.patch.object(subway, 'LOG')
    @mock.patch.object(subway.SubwayDaemon, 'update_limits')
    @mock.patch.object(subway, 'forward')
    def test_reload_zrange_fails(self, mock_forward, mock_update_limits,
                                 mock_LOG, mock_Semaphore):
        config = {}
        master = mock.Mock(**{'zrange.side_effect': TestException})
        slaves = ['slave1', 'slave2', 'slave3']
        sd = subway.SubwayDaemon(config, master, slaves)

        sd.reload()

        sd.pending.acquire.assert_called_once_with(False)
        sd.pending.release.assert_called_once_with()
        master.zrange.assert_called_once_with('limits', 0, -1)
        self.assertFalse(mock_forward.called)
        self.assertFalse(mock_update_limits.called)
        mock_LOG.exception.assert_called_once_with("Could not load limits")
        self.assertEqual(sd.limits, [])

    @mock.patch('eventlet.semaphore.Semaphore', return_value=mock.Mock(**{
        'acquire.return_value': True,
    }))
    @mock.patch.object(subway, 'LOG')
    @mock.patch.object(subway.SubwayDaemon, 'update_limits')
    @mock.patch.object(subway, 'forward')
    def test_reload_unchanged(self, mock_forward, mock_update_limits,
                              mock_LOG, mock_Semaphore):
        config = {}
        master = mock.Mock(**{'zrange.return_value': ['limit1', 'limit2']})
        slaves = ['slave1', 'slave2', 'slave3']
        sd = subway.SubwayDaemon(config, master, slaves)
        sd.limits = ['limit1', 'limit2']

        sd.reload()

        sd.pending.acquire.assert_called_once_with(False)
        sd.pending.release.assert_called_once_with()
        master.zrange.assert_called_once_with('limits', 0, -1)
        self.assertFalse(mock_forward.called)
        self.assertFalse(mock_update_limits.called)
        self.assertEqual(len(mock_LOG.method_calls), 0)
        self.assertEqual(sd.limits, ['limit1', 'limit2'])

    @mock.patch('eventlet.semaphore.Semaphore', return_value=mock.Mock(**{
        'acquire.return_value': True,
    }))
    @mock.patch.object(subway, 'LOG')
    @mock.patch.object(subway.SubwayDaemon, 'update_limits')
    @mock.patch.object(subway, 'forward')
    def test_reload_changed(self, mock_forward, mock_update_limits,
                            mock_LOG, mock_Semaphore):
        config = {}
        master = mock.Mock(**{'zrange.return_value': ['limit1', 'limit2']})
        slaves = ['slave1', 'slave2', 'slave3']
        sd = subway.SubwayDaemon(config, master, slaves)
        sd.limits = ['limit2', 'limit1']

        sd.reload()

        sd.pending.acquire.assert_called_once_with(False)
        sd.pending.release.assert_called_once_with()
        master.zrange.assert_called_once_with('limits', 0, -1)
        mock_forward.assert_called_once_with(sd, 'reload', None)
        mock_update_limits.assert_has_calls([
            mock.call('limits', 'slave1'),
            mock.call('limits', 'slave2'),
            mock.call('limits', 'slave3'),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 0)
        self.assertEqual(sd.limits, ['limit1', 'limit2'])

    @mock.patch('eventlet.semaphore.Semaphore', return_value=mock.Mock(**{
        'acquire.return_value': True,
    }))
    @mock.patch.object(subway, 'LOG')
    @mock.patch.object(subway.SubwayDaemon, 'update_limits')
    @mock.patch.object(subway, 'forward')
    def test_reload_changed_altkey(self, mock_forward, mock_update_limits,
                                   mock_LOG, mock_Semaphore):
        config = {'limits_key': 'alt_limits'}
        master = mock.Mock(**{'zrange.return_value': ['limit1', 'limit2']})
        slaves = ['slave1', 'slave2', 'slave3']
        sd = subway.SubwayDaemon(config, master, slaves)
        sd.limits = ['limit2', 'limit1']

        sd.reload()

        sd.pending.acquire.assert_called_once_with(False)
        sd.pending.release.assert_called_once_with()
        master.zrange.assert_called_once_with('alt_limits', 0, -1)
        mock_forward.assert_called_once_with(sd, 'reload', None)
        mock_update_limits.assert_has_calls([
            mock.call('alt_limits', 'slave1'),
            mock.call('alt_limits', 'slave2'),
            mock.call('alt_limits', 'slave3'),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 0)
        self.assertEqual(sd.limits, ['limit1', 'limit2'])

    @mock.patch('eventlet.semaphore.Semaphore', return_value=mock.Mock(**{
        'acquire.return_value': True,
    }))
    @mock.patch.object(subway, 'LOG')
    @mock.patch.object(subway.SubwayDaemon, 'update_limits')
    @mock.patch.object(subway, 'forward')
    def test_reload_changed_args(self, mock_forward, mock_update_limits,
                                 mock_LOG, mock_Semaphore):
        config = {}
        master = mock.Mock(**{'zrange.return_value': ['limit1', 'limit2']})
        slaves = ['slave1', 'slave2', 'slave3']
        sd = subway.SubwayDaemon(config, master, slaves)
        sd.limits = ['limit2', 'limit1']

        sd.reload("extra:args:for:forward")

        sd.pending.acquire.assert_called_once_with(False)
        sd.pending.release.assert_called_once_with()
        master.zrange.assert_called_once_with('limits', 0, -1)
        mock_forward.assert_called_once_with(sd, 'reload',
                                             'extra:args:for:forward')
        mock_update_limits.assert_has_calls([
            mock.call('limits', 'slave1'),
            mock.call('limits', 'slave2'),
            mock.call('limits', 'slave3'),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 0)
        self.assertEqual(sd.limits, ['limit1', 'limit2'])

    def test_update_limits(self):
        sd = subway.SubwayDaemon('config', 'master', 'slaves')
        sd.limits = ['limit1', 'limit3', 'limit5']
        pipeline = mock.MagicMock(**{
            'zrange.return_value': ['limit1', 'limit2', 'limit3'],
            'execute.side_effect': [
                redis.WatchError,
                None,
            ]
        })
        pipeline.__enter__.return_value = pipeline
        slave = mock.Mock(**{'pipeline.return_value': pipeline})

        sd.update_limits('limits', slave)

        slave.pipeline.assert_called_once_with()
        pipeline.assert_has_calls([
            mock.call.__enter__(),
            mock.call.watch('limits'),
            mock.call.zrange('limits', 0, -1),
            mock.call.multi(),
            mock.call.zrem('limits', 'limit2'),
            mock.call.zadd('limits', 10, 'limit1'),
            mock.call.zadd('limits', 20, 'limit3'),
            mock.call.zadd('limits', 30, 'limit5'),
            mock.call.execute(),
            mock.call.watch('limits'),
            mock.call.zrange('limits', 0, -1),
            mock.call.multi(),
            mock.call.zrem('limits', 'limit2'),
            mock.call.zadd('limits', 10, 'limit1'),
            mock.call.zadd('limits', 20, 'limit3'),
            mock.call.zadd('limits', 30, 'limit5'),
            mock.call.execute(),
            mock.call.__exit__(None, None, None),
        ])
        # Magic methods (__enter__, __exit__) apparently don't get
        # added to method_calls (?)
        self.assertEqual(len(pipeline.method_calls), 16)


class TestForward(unittest2.TestCase):
    def test_noargs(self):
        config = {}
        slaves = [
            mock.Mock(),
            mock.Mock(),
            mock.Mock(),
        ]
        sd = mock.Mock(config=config, slaves=slaves)

        subway.forward(sd, 'test', None)

        for slave in slaves:
            slave.publish.assert_called_once_with('control', 'test')

    def test_withargs(self):
        config = {}
        slaves = [
            mock.Mock(),
            mock.Mock(),
            mock.Mock(),
        ]
        sd = mock.Mock(config=config, slaves=slaves)

        subway.forward(sd, 'test', 'arguments:for:test')

        for slave in slaves:
            slave.publish.assert_called_once_with(
                'control', 'test:arguments:for:test')

    def test_withargs_altchan(self):
        config = {'channel': 'alternate'}
        slaves = [
            mock.Mock(),
            mock.Mock(),
            mock.Mock(),
        ]
        sd = mock.Mock(config=config, slaves=slaves)

        subway.forward(sd, 'test', 'arguments:for:test')

        for slave in slaves:
            slave.publish.assert_called_once_with(
                'alternate', 'test:arguments:for:test')


class TestRegister(unittest2.TestCase):
    @mock.patch.object(subway.SubwayDaemon, '_register')
    def test_register(self, mock_register):
        @subway.register("spam")
        def spam():
            pass

        mock_register.assert_called_once_with("spam", spam)


class TestReload(unittest2.TestCase):
    @mock.patch('random.random', return_value=0.5)
    @mock.patch('eventlet.spawn_after')
    @mock.patch('eventlet.spawn_n')
    def test_no_args(self, mock_spawn_n, mock_spawn_after, mock_random):
        config = {}
        sd = mock.Mock(config=config, reload='reloader')

        subway.reload(sd, 'reload', '')

        mock_spawn_n.assert_called_once_with('reloader', None)
        self.assertFalse(mock_spawn_after.called)
        self.assertFalse(mock_random.called)

    @mock.patch('random.random', return_value=0.5)
    @mock.patch('eventlet.spawn_after')
    @mock.patch('eventlet.spawn_n')
    def test_no_args_configured_spread(self, mock_spawn_n,
                                       mock_spawn_after, mock_random):
        config = {'reload_spread': '12'}
        sd = mock.Mock(config=config, reload='reloader')

        subway.reload(sd, 'reload', '')

        self.assertFalse(mock_spawn_n.called)
        mock_spawn_after.assert_called_once_with(
            6.0, 'reloader', 'spread:12.0')
        mock_random.assert_called_once_with()

    @mock.patch('random.random', return_value=0.5)
    @mock.patch('eventlet.spawn_after')
    @mock.patch('eventlet.spawn_n')
    def test_immediate(self, mock_spawn_n, mock_spawn_after, mock_random):
        config = {'reload_spread': '12'}
        sd = mock.Mock(config=config, reload='reloader')

        subway.reload(sd, 'reload', 'immediate')

        mock_spawn_n.assert_called_once_with('reloader', 'immediate')
        self.assertFalse(mock_spawn_after.called)
        self.assertFalse(mock_random.called)

    @mock.patch('random.random', return_value=0.5)
    @mock.patch('eventlet.spawn_after')
    @mock.patch('eventlet.spawn_n')
    def test_spread(self, mock_spawn_n, mock_spawn_after, mock_random):
        config = {'reload_spread': '12'}
        sd = mock.Mock(config=config, reload='reloader')

        subway.reload(sd, 'reload', 'spread:18.0')

        self.assertFalse(mock_spawn_n.called)
        mock_spawn_after.assert_called_once_with(
            9.0, 'reloader', 'spread:18.0')
        mock_random.assert_called_once_with()

    @mock.patch('random.random', return_value=0.5)
    @mock.patch('eventlet.spawn_after')
    @mock.patch('eventlet.spawn_n')
    def test_spread_bad_spread(self, mock_spawn_n,
                               mock_spawn_after, mock_random):
        config = {'reload_spread': '12'}
        sd = mock.Mock(config=config, reload='reloader')

        subway.reload(sd, 'reload', 'spread:18.0a')

        self.assertFalse(mock_spawn_n.called)
        mock_spawn_after.assert_called_once_with(
            6.0, 'reloader', 'spread:12.0')
        mock_random.assert_called_once_with()

    @mock.patch('random.random', return_value=0.5)
    @mock.patch('eventlet.spawn_after')
    @mock.patch('eventlet.spawn_n')
    def test_spread_unknown_type(self, mock_spawn_n,
                                 mock_spawn_after, mock_random):
        config = {'reload_spread': '12'}
        sd = mock.Mock(config=config, reload='reloader')

        subway.reload(sd, 'reload', 'unknown')

        self.assertFalse(mock_spawn_n.called)
        mock_spawn_after.assert_called_once_with(
            6.0, 'reloader', 'spread:12.0')
        mock_random.assert_called_once_with()


class TestGetDatabase(unittest2.TestCase):
    @mock.patch.object(redis, 'StrictRedis', return_value='handle')
    def test_missing_target(self, mock_StrictRedis):
        config = {}

        self.assertRaises(redis.ConnectionError, subway.get_database, config)
        self.assertFalse(mock_StrictRedis.called)

    @mock.patch.object(redis, 'StrictRedis', return_value='handle')
    def test_all_args(self, mock_StrictRedis):
        config = {
            'host': 'some_host',
            'port': '1234',
            'db': '18',
            'password': 'some_password',
            'socket_timeout': '5',
            'unix_socket_path': '/some/path',
            'extra_argument': 'some value',
        }

        result = subway.get_database(config)

        self.assertEqual(result, 'handle')
        mock_StrictRedis.assert_called_once_with(
            host='some_host',
            port=1234,
            db=18,
            password='some_password',
            socket_timeout=5,
            unix_socket_path='/some/path',
        )


def fake_get_database(config):
    if 'fail' in config:
        raise redis.ConnectionError(config['fail'])
    return config


class TestSubway(unittest2.TestCase):
    def setup_config(self, **sections):
        conf = dict((k.replace('_', ':'), v) for k, v in sections.items())

        def items(section):
            if section not in conf:
                raise ConfigParser.NoSectionError(section)
            return conf[section].items()

        def has_section(section):
            return section in conf

        def sections():
            return conf.keys()

        return mock.Mock(**{
            'items.side_effect': items,
            'has_section.side_effect': has_section,
            'sections.side_effect': sections,
        })

    @mock.patch.object(subway, 'LOG')
    @mock.patch('eventlet.monkey_patch')
    @mock.patch('ConfigParser.SafeConfigParser')
    @mock.patch.object(subway, 'get_database', side_effect=fake_get_database)
    @mock.patch.object(subway, 'SubwayDaemon',
                       return_value=mock.Mock(**{'reload': 'reloader'}))
    @mock.patch('eventlet.spawn_after')
    def test_no_master(self, mock_spawn_after, mock_SubwayDaemon,
                       mock_get_database, mock_SafeConfigParser,
                       mock_monkey_patch, mock_LOG):
        mock_SafeConfigParser.return_value = self.setup_config()

        self.assertRaises(subway.SubwayException, subway.subway, 'conf_file')
        mock_monkey_patch.assert_called_once_with()
        mock_SafeConfigParser.assert_called_once_with()
        mock_SafeConfigParser.return_value.assert_has_calls([
            mock.call.read(['conf_file']),
            mock.call.items('config'),
            mock.call.has_section('master'),
        ])
        self.assertFalse(mock_get_database.called)
        self.assertFalse(mock_SubwayDaemon.called)
        self.assertFalse(mock_spawn_after.called)
        self.assertEqual(len(mock_LOG.method_calls), 0)

    @mock.patch.object(subway, 'LOG')
    @mock.patch('eventlet.monkey_patch')
    @mock.patch('ConfigParser.SafeConfigParser')
    @mock.patch.object(subway, 'get_database', side_effect=fake_get_database)
    @mock.patch.object(subway, 'SubwayDaemon',
                       return_value=mock.Mock(**{'reload': 'reloader'}))
    @mock.patch('eventlet.spawn_after')
    def test_no_slaves(self, mock_spawn_after, mock_SubwayDaemon,
                       mock_get_database, mock_SafeConfigParser,
                       mock_monkey_patch, mock_LOG):
        mock_SafeConfigParser.return_value = self.setup_config(
            master=dict(host='master'),
        )

        self.assertRaises(subway.SubwayException, subway.subway, 'conf_file')
        mock_monkey_patch.assert_called_once_with()
        mock_SafeConfigParser.assert_called_once_with()
        mock_SafeConfigParser.return_value.assert_has_calls([
            mock.call.read(['conf_file']),
            mock.call.items('config'),
            mock.call.has_section('master'),
            mock.call.items('master'),
            mock.call.sections(),
        ])
        mock_get_database.assert_has_calls([
            mock.call(dict(host='master')),
        ])
        self.assertFalse(mock_SubwayDaemon.called)
        self.assertFalse(mock_spawn_after.called)
        self.assertEqual(len(mock_LOG.method_calls), 0)

    @mock.patch.object(subway, 'LOG')
    @mock.patch('eventlet.monkey_patch')
    @mock.patch('ConfigParser.SafeConfigParser')
    @mock.patch.object(subway, 'get_database', side_effect=fake_get_database)
    @mock.patch.object(subway, 'SubwayDaemon',
                       return_value=mock.Mock(**{'reload': 'reloader'}))
    @mock.patch('eventlet.spawn_after')
    def test_normal(self, mock_spawn_after, mock_SubwayDaemon,
                    mock_get_database, mock_SafeConfigParser,
                    mock_monkey_patch, mock_LOG):
        mock_SafeConfigParser.return_value = self.setup_config(
            config=dict(a=1, b=2, c=3),
            master=dict(host='master'),
            slave_1=dict(host='slave1'),
        )

        subway.subway('conf_file')
        mock_monkey_patch.assert_called_once_with()
        mock_SafeConfigParser.assert_called_once_with()
        mock_SafeConfigParser.return_value.assert_has_calls([
            mock.call.read(['conf_file']),
            mock.call.items('config'),
            mock.call.has_section('master'),
            mock.call.items('master'),
            mock.call.sections(),
            mock.call.items('slave:1'),
        ])
        mock_get_database.assert_has_calls([
            mock.call(dict(host='master')),
            mock.call(dict(host='slave1')),
        ])
        mock_SubwayDaemon.assert_called_once_with(
            dict(a=1, b=2, c=3), dict(host='master'), [dict(host='slave1')])
        mock_spawn_after.assert_called_once_with(2.0, 'reloader')
        mock_SubwayDaemon.return_value.listen.assert_called_once_with()
        self.assertEqual(len(mock_LOG.method_calls), 0)

    @mock.patch.object(subway, 'LOG')
    @mock.patch('eventlet.monkey_patch')
    @mock.patch('ConfigParser.SafeConfigParser')
    @mock.patch.object(subway, 'get_database', side_effect=fake_get_database)
    @mock.patch.object(subway, 'SubwayDaemon',
                       return_value=mock.Mock(**{'reload': 'reloader'}))
    @mock.patch('eventlet.spawn_after')
    def test_slave_connect_fail(self, mock_spawn_after, mock_SubwayDaemon,
                                mock_get_database, mock_SafeConfigParser,
                                mock_monkey_patch, mock_LOG):
        mock_SafeConfigParser.return_value = self.setup_config(
            config=dict(a=1, b=2, c=3),
            master=dict(host='master'),
            slave_1=dict(fail='slave1'),
        )

        self.assertRaises(subway.SubwayException, subway.subway, 'conf_file')
        mock_monkey_patch.assert_called_once_with()
        mock_SafeConfigParser.assert_called_once_with()
        mock_SafeConfigParser.return_value.assert_has_calls([
            mock.call.read(['conf_file']),
            mock.call.items('config'),
            mock.call.has_section('master'),
            mock.call.items('master'),
            mock.call.sections(),
            mock.call.items('slave:1'),
        ])
        mock_get_database.assert_has_calls([
            mock.call(dict(host='master')),
            mock.call(dict(fail='slave1')),
        ])
        self.assertFalse(mock_SubwayDaemon.called)
        self.assertFalse(mock_spawn_after.called)
        mock_LOG.assert_has_calls([
            mock.call.exception("Failed to connect to slave '1'"),
        ])
        self.assertEqual(len(mock_LOG.method_calls), 1)
