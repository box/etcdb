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

    $packages = ['vim-enhanced', 'python2-pip', 'jq', 'gcc', 'python-devel']

    package { $packages:
        ensure => installed,
        require => [
            Package['epel-release']
        ]
    }

    file { '/usr/bin/pip-python':
        ensure => 'link',
        target => '/usr/bin/pip',
        require => Package['python2-pip']
    }

    package { ['tox']:
        ensure => installed,
        provider => pip,
        require => [
            Package['python2-pip'],
            File['/usr/bin/pip-python']
        ]
    }
}
