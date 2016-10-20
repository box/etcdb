class profile::infra2 {
    include profile::base
    include profile::etcdnode

    file { '/etc/etcd/etcd.conf':
        ensure => present,
        owner => 'root',
        group => 'root',
        mode => '644',
        source => 'puppet:///modules/profile/etcd-infra2.conf',
        require => File['/etc/etcd']
    }
}
