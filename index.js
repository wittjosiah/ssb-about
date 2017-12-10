var FlumeReduce = require('flumeview-reduce')
var ref = require('ssb-ref')
var ssbKeys = require('ssb-keys')

exports.name = 'about'
exports.version = require('./package.json').version
exports.manifest = {
  stream: 'source',
  get: 'async',
  tagsStream: 'source',
  tagsGet: 'async'
}

exports.init = function (ssb, config) {
  var about = ssb._flumeUse('about', FlumeReduce(1, reduce, map))
  var tags = ssb._flumeUse('tags', FlumeReduce(1, tagsReduce, tagsMap))

  return {
    stream: about.stream,
    get: about.get,
    tagsStream: tags.stream,
    tagsGet: tags.get 
  }

  function unbox (msg) {
    if (typeof msg.value.content === 'string') {
      var value = unboxValue(msg.value)
      if (value) {
        return {
          key: msg.key, value: value, timestamp: msg.timestamp
        }
      }
    }
    return msg
  }

  function unboxValue (value) {
    var plaintext = null
    try {
      plaintext = ssbKeys.unbox(value.content, ssb.keys.private)
    } catch (ex) {}
    if (!plaintext) return null
    return {
      previous: value.previous,
      author: value.author,
      sequence: value.sequence,
      timestamp: value.timestamp,
      hash: value.hash,
      content: plaintext,
      private: true
    }
  }

  function tagsReduce (result, item) {
    if (!result) result = {}
    if (item) {
      for (var author in item) {
        var valuesForAuthor = result[author] = result[author] || {}
        var tagsForAuthor = valuesForAuthor['tags'] = valuesForAuthor['tags'] || {}
        var msgsForAuthor = valuesForAuthor['msgs'] = valuesForAuthor['msgs'] || {}
        for (var tag in item[author]['tags']) {
          var msgsForTag = tagsForAuthor[tag] = tagsForAuthor[tag] || {}
          for (var msg in item[author]['tags'][tag]) {
            var timestamp = item[author]['tags'][tag][msg]
            if (!msgsForTag[msg] || timestamp > msgsForTag[msg]) {
              msgsForTag[msg] = timestamp
            }
          }
        }
        for (var msg in item[author]['msgs']) {
          var tagsForMsg = msgsForAuthor[msg] = msgsForAuthor[msg] || {}
          var nextTags = item[author]['msgs'][msg]
          var timestamp = Object.values(nextTags)[0]
          for (var tag in tagsForMsg) {
            if (timestamp && !(tag in nextTags) && timestamp > tagsForMsg[tag]) {
              tagsForAuthor[tag][msg] = null
            }
          }
          msgsForAuthor[msg] = nextTags
        }
      }
    }
    return result
  }

  function tagsMap (boxedMsg) {
    var msg = unbox(boxedMsg)
    if (msg.value.content && msg.value.content.type === 'about' && msg.value.content.tags && ref.isLink(msg.value.content.about)) {
      var author = msg.value.author
      var target = msg.value.content.about
      var tags = {}
      var msgs = {
        [target]: {}
      }

      for (var index in msg.value.content.tags) {
        var tag = msg.value.content.tags[index]
        tags[tag] = {
          [target]: msg.value.timestamp
        }
        msgs[target][tag] = msg.value.timestamp
      }

      return {
        [author]: {
          'tags': tags,
          'msgs': msgs
        }
      }
    }
  }

  function reduce (result, item) {
    if (!result) result = {}
    if (item) {
      for (var target in item) {
        var valuesForId = result[target] = result[target] || {}
        for (var key in item[target]) {
          var valuesForKey = valuesForId[key] = valuesForId[key] || {}
          for (var author in item[target][key]) {
            var value = item[target][key][author]
            if (!valuesForKey[author] || value[1] > valuesForKey[author][1]) {
              valuesForKey[author] = value
            }
          }
        }
      }
    }
    return result
  }

  function map (boxedMsg) {
    var msg = unbox(boxedMsg)
    if (msg.value.content && msg.value.content.type === 'about' && ref.isLink(msg.value.content.about)) {
      var author = msg.value.author
      var target = msg.value.content.about
      var values = {}

      for (var key in msg.value.content) {
        if (key !== 'about' && key !== 'type') {
          values[key] = {
            [author]: [msg.value.content[key], msg.value.timestamp]
          }
        }
      }

      return {
        [target]: values
      }
    }
  }
}
