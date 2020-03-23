local json = require "json"
local lrc = require "lredis.cqueues"
local cqueues = require "cqueues"

function Dobaos(req_channel, res_prefix, cast_channel)
  local d = {}
  d.req_channel = req_channel
  d.cast_channel = cast_channel
  d.res_prefix = res_prefix

  d.pub = lrc.connect_tcp()
  d.sub = lrc.connect_tcp()
  d.cast = lrc.connect_tcp()
  d.res_cq = cqueues.new()
  d.cast_cq = cqueues.new()

  d.res_subscribed = false
  d.cast_subscribed = false

  d.last_res_channel = ""
  d.res_received = false
  d.res_payload = Nil
  d.res_timeout = false
  d.res_timetick = 0

  d.cast_received = false
  d.cast_payload = Nil

  d.res_cq:wrap(function()
    d.sub:psubscribe(d.res_prefix .. "*")
    while true do
      local item = d.sub:get_next()
      if item == nil then
        -- do nothing
      else
        local message_type = item[1]
        if message_type == "pmessage" then
          if item[3] == d.last_res_channel then
            d.res_received = true
            d.res_payload = json.parse(item[4])
          end
        elseif message_type == "psubscribe" then
          d.res_subscribed = true
        end
      end
    end
  end)
  while not d.res_subscribed do
    d.res_cq:step(0.001)
  end

  d.cast_cq:wrap(function()
    d.cast:subscribe(d.cast_channel)
    while true do
      local item = d.cast:get_next()
      if item == nil then
        -- do nothing
      else
        local message_type = item[1]
        if message_type == "message" then
          if item[2] == d.cast_channel then
            d.cast_received = true
            d.cast_payload = json.parse(item[3])
          end
        elseif message_type == "subscribe" then
          d.cast_subscribed = true
        end
      end
    end
  end)
  while not d.cast_subscribed do
    d.cast_cq:step(0.001)
  end

  function d:common_request(method, payload)
    self.res_payload = Nil
    self.res_timeout = false
    self.res_timetick = 0
    self.last_res_channel = ""
    local data = {}
    data.response_channel = self.res_prefix .. math.random(1, 255)
    data.method = method
    data.payload = payload
    msg = json.stringify(data)
    self.pub:call("publish", self.req_channel, msg)
    self.last_res_channel = data.response_channel
    while not self.res_received and not self.res_timeout do
      self.res_cq:step(0.001)
      self.res_timetick = d.res_timetick + 1
      self.res_timeout = d.res_timetick > 5000
    end
    if self.res_received then
      self.res_received = false
      self.res_timeout = false
      self.res_timetick = 0
      self.last_res_channel = ""

      result = self.res_payload
      if result["method"] == "success" then
        return result["payload"]
      elseif result["method"] == "error" then
        error(result["payload"])
      end
    end
    if self.res_timeout then
      error("ERR_REQ_TIMEOUT")
    end
  end

  function d:get_description(payload)
    return self:common_request("get description", payload)
  end

  function d:get_value(payload)
    return self:common_request("get value", payload)
  end

  function d:set_value(payload)
    return self:common_request("set value", payload)
  end

  function d:put_value(payload)
    return self:common_request("put value", payload)
  end

  function d:read_value(payload)
    return self:common_request("read value", payload)
  end

  function d:get_progmode()
    return self:common_request("get programming mode", 0)
  end

  function d:set_progmode(value)
    return self:common_request("set programming mode", value)
  end

  function d:process_cast()
    self.cast_cq:step(0.001)
    if self.cast_received then
      self.cast_received = false
      return self.cast_payload
    else
      return Nil
    end
  end

  return d
end

return Dobaos
