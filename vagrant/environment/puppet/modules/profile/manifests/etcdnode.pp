class profile::etcdnode {
    include profile::base

    package { ['etcd']:
        ensure => latest
    }

    service { 'etcd':
        ensure => running,
        require => [
            Package['etcd'],
            File['/etc/etcd/etcd.conf']
        ]
    }

    file { '/etc/etcd':
        ensure => directory,
        owner => 'root',
        mode => '755'
    }
}
