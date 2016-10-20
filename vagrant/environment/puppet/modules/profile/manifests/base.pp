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
        provider => rpm,
        source => 'http://dl.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-8.noarch.rpm'
    }

    $packages = [ 'vim-enhanced', 'python-pip']

    package { $packages:
        ensure => installed,
        require => [Package['epel-release']]
    }

    package { ['tox']:
        ensure => installed,
        provider => pip,
        require => Package['python-pip']
    }
}
