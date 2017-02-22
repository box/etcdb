class profile::etcdnode {
    include profile::base

    package { ['etcd']:
        ensure => '2.3.7-4.el7'
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
