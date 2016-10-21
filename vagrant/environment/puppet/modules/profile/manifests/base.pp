class profile::base {
    $user = 'vagrant'
    user { $user:
        ensure => present
    }

    file { "/home/${user}":
        ensure => directory,
        owner  => $user,
        mode   => "0750"
    }

    file { "/home/${profile::base::user}/.bashrc":
        ensure => present,
        owner  => $profile::base::user,
        mode   => "0644",
        source => 'puppet:///modules/profile/bashrc',
    }

    package { 'epel-release':
        ensure => installed
    }

    package { 'ius-release':
        ensure => installed,
        provider => rpm,
        source => 'https://centos6.iuscommunity.org/ius-release.rpm'

    }

    $packages = ['vim-enhanced', 'python-pip', 'python27']

    package { $packages:
        ensure => installed,
        require => [
            Package['epel-release'],
            Package['ius-release']
        ]
    }

    file { '/usr/bin/pip-python':
        ensure => 'link',
        target => '/usr/bin/pip',
        require => Package['python-pip']
    }

    package { ['tox']:
        ensure => installed,
        provider => pip,
        require => [
            Package['python-pip'],
            File['/usr/bin/pip-python']
        ]
    }
}
