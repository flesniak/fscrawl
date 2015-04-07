#ifndef HASHER_H
#define HASHER_H

#include <string>

using namespace std;

class Hasher {
public:
  enum hashType_t { noHash, md5, sha1, tth };
  enum hashStatus_t { hashSuccess, hashError, noHashSelected };

  Hasher(hashType_t type = noHash);
  void setHashType(hashType_t type);
  hashType_t getHashType() const;

  hashStatus_t hash(const string& filename, string& hash);

private:
  hashType_t p_hashType;
};

#endif //HASHER_H