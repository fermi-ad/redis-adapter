//
//  HiredisReply.cpp
//
//  Stream reply parsing implementation
//

#include "HiredisReply.hpp"

Attrs parse_entry_fields(redisReply* fields_reply)
{
  Attrs attrs;
  if (!fields_reply || fields_reply->type != REDIS_REPLY_ARRAY) return attrs;

  for (size_t i = 0; i + 1 < fields_reply->elements; i += 2)
  {
    redisReply* key = fields_reply->element[i];
    redisReply* val = fields_reply->element[i + 1];
    if (key && val && key->type == REDIS_REPLY_STRING && val->type == REDIS_REPLY_STRING)
    {
      attrs.emplace(std::string(key->str, key->len),
                    std::string(val->str, val->len));
    }
  }
  return attrs;
}

TimeAttrsList parse_stream_reply(redisReply* reply)
{
  TimeAttrsList result;
  if (!reply || reply->type != REDIS_REPLY_ARRAY) return result;

  for (size_t i = 0; i < reply->elements; ++i)
  {
    redisReply* entry = reply->element[i];
    if (!entry || entry->type != REDIS_REPLY_ARRAY || entry->elements < 2) continue;

    redisReply* id_reply = entry->element[0];
    redisReply* fields_reply = entry->element[1];

    if (!id_reply || id_reply->type != REDIS_REPLY_STRING) continue;

    std::string id(id_reply->str, id_reply->len);
    Attrs attrs = parse_entry_fields(fields_reply);
    result.emplace_back(RAL_Time(id), std::move(attrs));
  }
  return result;
}

std::unordered_map<std::string, TimeAttrsList> parse_xread_reply(redisReply* reply)
{
  std::unordered_map<std::string, TimeAttrsList> result;
  if (!reply || reply->type != REDIS_REPLY_ARRAY) return result;

  // XREAD returns: [ [stream_name, [[id, [fields]], ...]], ... ]
  for (size_t i = 0; i < reply->elements; ++i)
  {
    redisReply* stream = reply->element[i];
    if (!stream || stream->type != REDIS_REPLY_ARRAY || stream->elements < 2) continue;

    redisReply* name_reply = stream->element[0];
    redisReply* entries_reply = stream->element[1];

    if (!name_reply || name_reply->type != REDIS_REPLY_STRING) continue;

    std::string name(name_reply->str, name_reply->len);
    result[name] = parse_stream_reply(entries_reply);
  }
  return result;
}
