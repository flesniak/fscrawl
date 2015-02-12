pkgname=fscrawl-git
pkgver=r48.c3bb71b
pkgrel=1
pkgdesc="A tool to create a tree-structure representing the local filesystem in a mysql database"
url="http://github.com/flesniak/fscrawl"
arch=('x86_64' 'i686')
license=('GPL3')
depends=('mysql-connector-c++')
makedepends=('git')
conflicts=('fscrawl')
source=("${pkgname}::git+https://github.com/flesniak/fscrawl.git")
md5sums=('SKIP')

pkgver() {
  cd "$srcdir/$pkgname"
  ( set -o pipefail
    git describe --long --tags 2>/dev/null | sed 's/\([^-]*-g\)/r\1/;s/-/./g' ||
    printf "r%s.%s" "$(git rev-list --count HEAD)" "$(git rev-parse --short HEAD)"
  )
}

build() {
  cd ${pkgname}
  make
}

package() {
  install -Dm755 "${pkgname}/fscrawl" "${pkgdir}"/usr/bin/fscrawl
}
