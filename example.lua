local Dobaos = require "dobaos"
local json = require "json"

-- hello, friend
print("hello, friend")
cli = Dobaos("dobaos_req", "dobalua_res_", "dobaos_cast")

print("get val: ", cli:get_value(1))
print("read val: ", cli:read_value(1))
print("get descr 1 2 3: ", cli:get_description({1, 2, 3}))
print("get descr *: ", cli:get_description("*"))

local progmode = cli:get_progmode()
print("get progmode: ", progmode)
print("set progmode: ", cli:set_progmode(not progmode)) -- print true if success
print("get progmode: ", cli:get_progmode())

function process_value(payload)
  print("process_value: ", json.stringify(payload))
  local id1 = payload.id
  local value1 = payload.value
  -- simple AND. inputs are datapoints 2 and 3, output - 4
  if id1 == 2 or id1 == 3 then
    local id2
    if id1 == 2 then id2 = 3 else id2 = 2 end
    value2 = cli:get_value(id2).value
    local value3 = value1 or value2
    cli:set_value({ id = 4, value = value3 })
  end
end

while true do
  local cast = cli:process_cast()
  if cast ~= Nil then
    if cast.method == "datapoint value" then
      if cast.payload[1] == Nil then
        -- payload: { id: 1, value: vv }
        process_value(cast.payload)
      else
        -- payload: [{ id: 1, value: vv}]
        for k,v in ipairs(cast.payload) do
          process_value(v)
        end
      end
    end
  end
end
