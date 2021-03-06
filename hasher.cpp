#include "hasher.h"

#include <cerrno>
#include <cstring>

#include <rhash.h>

#include "logger.h"

Hasher::Hasher(hashType_t type) : p_hashType(type) {
  LOG(logDebug) << "Initializing hasher library, rhash";
  rhash_library_init();
}

void Hasher::setHashType(hashType_t type) {
  p_hashType = type;
}

Hasher::hashType_t Hasher::getHashType() const {
  return p_hashType;
}

Hasher::hashStatus_t Hasher::hash(const string& filename, string& hash) {
  unsigned rhashType;
  switch( p_hashType ) {
    case tth: rhashType = RHASH_TTH; break;
    case md5: rhashType = RHASH_MD5; break;
    case sha1: rhashType = RHASH_SHA1; break;
    default:
      LOG(logError) << "Hasher called with no hash algorithm selected";
      return noHashSelected;
  }

  unsigned char digest[64];
  int res = rhash_file(rhashType, filename.c_str(), digest);
  if( res < 0 ) {
    LOG(logError) << "LibRHash error: " << strerror(errno);
    return hashError;
  }

  char output[130];
  size_t length = 0;
  if( p_hashType == tth )
    length = rhash_print_bytes(output, digest, rhash_get_digest_size(rhashType), RHPR_BASE32);
  else
    length = rhash_print_bytes(output, digest, rhash_get_digest_size(rhashType), RHPR_HEX);
  hash = string(output, length);

  LOG(logDetailed) << "Calculated " << rhash_get_name(rhashType) << " hash of file " << filename << ": " << hash;
  return hashSuccess;
}

string Hasher::hashTypeToString(hashType_t type) {
  switch (type) {
    case Hasher::noHash : return "none";
    case Hasher::md5 : return "md5";
    case Hasher::sha1 : return "sha1";
    case Hasher::tth : return "tth";
    default : return "invalid";
  }
}
