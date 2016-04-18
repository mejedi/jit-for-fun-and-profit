local front = require('schema_front')
local c     = require('schema_c')

local format, find, gsub = string.format, string.find, string.gsub
local insert, remove, concat = table.insert, table.remove, table.concat

local f_create_schema    = front.create_schema
local f_validate_data    = front.validate_data
local f_create_ir        = front.create_ir

-- We give away a handle but we never expose schema data.
local schema_by_handle = setmetatable( {}, { __mode = 'k' } )

local function get_schema(handle)
    local schema = schema_by_handle[handle]
    if not schema then
        error(format('Not a schema: %s', handle), 0)
    end
    return schema
end

local function is_schema(schema_handle)
    return not not schema_by_handle[schema_handle]
end

-- IR-s are cached
local ir_by_key        = setmetatable( {}, { __mode = 'v' } )

local function get_ir(from_schema, to_schema, inverse)
    k = format('%s%p.%p', inverse and '-' or '', from_schema, to_schema)
    ir = ir_by_key[k]
    if ir then
        if type(ir) == 'table' and ir[1] == 'ERR' then
            return false, ir[2]
        else
            return true, ir
        end
    else
        local err
        ir, err = f_create_ir(from_schema, to_schema, inverse)
        if not ir then
            ir_by_key[k] = { 'ERR', err }
            return false, err
        else
            ir_by_key[k] = ir
            return true, ir
        end
    end
end

local function schema_to_string(handle)
    local schema = schema_by_handle[handle]
    return format('Schema (%s)',
                  handle[1] or (type(schema) ~= 'table' and schema) or
                  schema.name or schema.type or 'union')
end

local schema_handle_mt = {
    __tostring  = schema_to_string,
    __serialize = schema_to_string
}

local function create(raw_schema)
    local ok, schema = pcall(f_create_schema, raw_schema)
    if not ok then
        return false, schema
    end
    local schema_handle = setmetatable({}, schema_handle_mt)
    schema_by_handle[schema_handle] = schema
    return true, schema_handle
end

local function validate(schema_handle, data)
    return f_validate_data(get_schema(schema_handle), data)
end

local function are_compatible(schema_h1, schema_h2, opt_mode)
    local ok, extra = get_ir(get_schema(schema_h1), get_schema(schema_h2),
                             opt_mode == 'downgrade')
    if ok then
        return true -- never leak IR
    else
        return false, extra
    end
end

-- compile(schema)
-- compile(schema1, schema2)
-- compile({schema1, schema2, downgrade = true, extra_fields = { ... }})
-- --> { deflate = , inflate = , xdeflate = , convert_deflated = , convert_inflated = }
local function compile(...)
    local n = select('#', ...)
    local args = { ... } 
    if n == 1 and not is_schema(args[1]) then
        if type(args[1]) ~= 'table' then
            error('Expecting a schema or a table', 0)
        end
        n = select('#', unpack(args[1]))
        args = args[1]
    end
    local list = {}
    for i = 1, n do
        insert(list, get_schema(args[i]))
    end
    if #list == 0 then
        error('Expecting a schema', 0)
    end
end

return {
    are_compatible = are_compatible,
    create         = create,
    compile        = compile,
    is_schema      = is_schema,
    validate       = validate
}
