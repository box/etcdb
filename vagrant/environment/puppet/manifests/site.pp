node /^client$/ {
  include role::client
}

node /^infra0$/ {
  include role::infra0
}

node /^infra1$/ {
    include role::infra1
}

node /^infra2$/ {
    include role::infra2
}
