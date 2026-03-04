//
//  HiredisReply.hpp
//
//  RAII wrapper for redisReply* and stream reply parsing utilities
//

#pragma once

#include <hiredis/hiredis.h>
#include "RAL_Types.hpp"
#include <memory>

// RAII unique_ptr for redisReply
struct ReplyDeleter { void operator()(redisReply* r) const { if (r) freeReplyObject(r); } };
using ReplyPtr = std::unique_ptr<redisReply, ReplyDeleter>;

// Parse field-value array into Attrs
// fields_reply is an ARRAY: [field, value, field, value, ...]
Attrs parse_entry_fields(redisReply* fields_reply);

// Parse XRANGE / XREVRANGE reply into TimeAttrsList
// reply is an ARRAY of [id_string, fields_array] entries
TimeAttrsList parse_stream_reply(redisReply* reply);

// Parse XREAD reply into map of stream_key -> TimeAttrsList
// reply is an ARRAY of [stream_name, entries_array]
std::unordered_map<std::string, TimeAttrsList> parse_xread_reply(redisReply* reply);
