class profile::etcdnode {
    include profile::base

    package { ['etcd']:
        ensure   => installed,
        source   => '/tmp/etcd-2.3.7-4.el7.x86_64.rpm',
        provider => 'rpm',
        require  => File['/tmp/etcd-2.3.7-4.el7.x86_64.rpm']
    }

    file { '/tmp/etcd-2.3.7-4.el7.x86_64.rpm':
        ensure => present,
        source => 'puppet:///modules/profile/etcd-2.3.7-4.el7.x86_64.rpm'
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
