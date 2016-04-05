local format, find, gsub = string.format, string.find, string.gsub
local insert, remove, concat = table.insert, table.remove, table.concat

local primitive_type = {
    null = 1, boolean = 1, int = 1, long = 1, float = 1, double = 1,
    bytes = 1, string = 1
}

local function schema_error(ss, fmt, ...)
    local msg = format(fmt, ...)
    error(msg)
end

local function validname(name)
    return gsub(name, '[_A-Za-z][_0-9A-Za-z]*', '-') == '-'
end

local function fullname(name, ss)
    if find(name, '%.') then
        return name
    end
    -- digg ss for a namespace 
    local i = #ss
    while i > 0 do
        local nsname = ss[i].name
        i = i - 1
        if nsname then
            if not find(nsname, '%.') then
                return name
            end
            return gsub(nsname, '[^.]*$', '') .. name
        end
    end
    return name
end

local function validfullname(name)
    return gsub(gsub(name, '[_A-Za-z][_0-9A-Za-z]*', '-'), '-%.', '') == '-'
end

local function checkname(schema, ss, scope)
    local xname = schema.name
    if not xname then
        schema_error(ss, 'Must have a "name"') 
    end
    xname = tostring(xname)
    if not find(xname, '%.') then
        local xns = schema.namespace
        if xns then
            xname = format('%s.%s', xns, xname)
        end
    end
    if not validfullname(xname) then
        schema_error(ss, 'Bad name: %s', xname)
    end
    if primitive_type[gsub(xname, '.*%.', '')] then
        schema_error(ss, 'Redefining primitive type name: %s', xname)
    end
    xname = fullname(xname, ss)
    if scope[xname] then
        schema_error(ss, 'Name already defined: %s', xname)
    end
    if scope['@'..xname] then
        schema_error(ss, 'Alias already defined: %s', xname)
    end
    return xname
end

local function checkaliases(schema, ss, scope)
    local xaliases = schema.aliases
    if not xaliases then
        return
    end
    if type(xaliases) ~= 'table' then
        schema_error(ss, 'Property "aliases" must be a table')
    end
    if #xaliases == 0 then
        return
    end
    local aliases = {}
    for _, alias in ipairs(xaliases) do
        alias = tostring(alias)
        if not validfullname(alias) then
            schema_error(ss, 'Bad name: %s', alias)
        end
        alias = fullname(alias, ss)
        if scope[alias] then
            schema_error(ss, 'Name already defined: %s', alias)
        end
        if scope['@'..alias] then
            schema_error(ss, 'Alias already defined: %s', alias)
        end
        insert(aliases, alias)
    end
    return aliases
end

local create_schema1

create_schema1 = function(schema, ss, scope)
    if ss[schema] then
        -- this check is necessary for unnamed complex types (union, array, map)
        schema_error(nil, 'Infinitely recursive schema')
    end
    if type(schema) == 'table' then
        if #schema > 0 then
            local res = { type = 'union', branches = {} }
            local u = {}
            insert(ss, res)
            ss[schema] = true
            for i, v in ipairs(schema) do
                res.branches[i] = {}
                local branch = create_schema1(v, ss, scope)
                res.branches[i] = branch
                local bxtype = branch.type
                if bxtype == 'union' then
                    schema_error(ss, 'Union may not immediately contain other unions')
                end
                local bxid = branch.name or bxtype
                if u[bxid] then
                    schema_error(ss, 'Union contains "%s" twice', bxid)
                end
                u[bxid] = true
            end
            remove(ss)
            ss[schema] = nil
            return res
        else
            if not pairs(schema) (schema, nil) then
                schema_error(ss, 'Union type must have at least one branch')
            end
            local xtype = schema.type
            if not xtype then
                schema_error(ss, 'Must have a "type"')
            end
            xtype = tostring(xtype)
            if primitive_type[xtype] then
                return { type = xtype }
            elseif xtype == 'record' then
                local res = { type = 'record', fields = {} }
                insert(ss, res)
                local name = checkname(schema, ss, scope)
                scope[name] = res
                res.name = name
                res.aliases = checkaliases(schema, ss, scope)
                local xfields = schema.fields
                if not xfields then
                    schema_error(ss, 'Record type must have "fields"')
                end
                if type(xfields) ~= 'table' then
                    schema_error(ss, 'Record "fields" must be a table')
                end
                if #xfields == 0 then
                    schema_error(ss, 'Record type must have at least one field')
                end
                local u, r = {}
                for _, xfield in ipairs(xfields) do
                    local field = {}
                    insert(res.fields, field)
                    if type(xfield) ~= 'table' then
                        schema_error(ss, 'Record field must be a table')
                    end
                    local xname = xfield.name
                    if not xname then
                        schema_error(ss, 'Record field must have a "name"')
                    end
                    xname = tostring(xname)
                    if not validname(xname) then
                        schema_error(ss, 'Bad record field name: %s', xname)
                    end
                    if u[xname] then
                        schema_error(ss, 'Record contains field "%s" twice', xname)
                    end
                    u[xname] = true
                    field.name = xname
                    local xtype = xfield.type
                    if not xtype then
                        schema_error(ss, 'Record field must have a "type"')
                    end
                    field.type = create_schema1(xtype, ss, scope)
                    if field.type.type == 'record' then
                        if not r then
                            r = {}
                            local i = #ss
                            while i > 0 and ss[i].type == 'record' do
                                r[ss[i].name] = i
                                i = i - 1
                            end
                        end
                        local i = r[field.type.name]
                        if i then
                            local path = {}
                            while ss[i] do
                                local fields = ss[i].fields
                                i = i + 1
                                insert(path, fields[#fields].name)
                            end
                            schema_error(nil, 'Record %s contains itself via %s',
                                         field.type.name, concat(path, '.'))
                        end
                    end
                    field.default = xfield.default -- defaults checked later
                    local xaliases = xfield.aliases
                    if xaliases then
                        if type(xaliases) ~= 'table' then
                            schema_error('Property "aliases" must be a table')
                        end
                        local aliases = {}
                        for _, alias in ipairs(xaliases) do
                            alias = tostring(alias)
                            if not validname(alias) then
                                schema_error(ss, 'Bad field alias name: %s', alias)
                            end
                            if u[alias] then
                                schema_error(ss, 'Alias name already defined: %s', alias)
                            end
                            u[alias] = true
                            insert(aliases, alias)
                        end
                        if #aliases ~= 0 then
                            field.aliases = aliases
                        end
                    end
                end
                remove(ss)
                return res
            elseif xtype == 'enum' then
                local res = { type = 'enum', symbols = {} }
                insert(ss, res)
                local name = checkname(schema, ss, scope)
                scope[name] = res
                res.name = name
                res.aliases = checkaliases(schema, ss, scope)
                local xsymbols = schema.symbols
                if not xsymbols then
                    schema_error(ss, 'Enum type must have "symbols"')
                end
                if type(xsymbols) ~= 'table' then
                    schema_error(ss, 'Enum "symbols" must be a table')
                end
                if #xsymbols == 0 then
                    schema_error(ss, 'Enum type must contain at least one symbol')
                end
                local u = {}
                for _, v in ipairs(xsymbols) do
                    v = tostring(v)
                    if not validname(v) then
                        schema_error(ss, 'Bad enum symbol name: %s', v)
                    end
                    if u[v] then
                        schema_error(ss, 'Enum contains symbol %s twice', v)
                    end
                    u[v] = true
                    insert(res.symbols, v)
                end
                remove(ss)
                return res
            elseif xtype == 'array' then
                local res = { type = 'array' }
                insert(ss, res)
                ss[schema] = true
                local xitems = schema.items
                if not xitems then
                    schema_error(ss, 'Array type must have "items"')
                end
                res.items = create_schema1(xitems, ss, scope)
                remove(ss)
                ss[schema] = nil
                return res
            elseif xtype == 'map' then
                local res = { type = 'map' }
                insert(ss, res)
                ss[schema] = true
                local xvalues = schema.values
                if not xvalues then
                    schema_error(ss, 'Map type must have "values"')
                end
                res.items = create_schema1(xvalues, ss, scope)
                remove(ss)
                ss[schema] = nil
                return res
            elseif xtype == 'fixed' then
                local res = { type = 'fixed' }
                insert(ss, res)
                local name = checkname(schema, ss, scope)
                scope[name] = res
                res.name = name
                res.aliases = checkaliases(schema, ss, scope)
                local xsize = schema.size
                if not xsize then
                    schema_error(ss, 'Fixed type must have "size"')
                end
                if type(xsize) ~= 'number' or xsize < 1 or math.floor(xsize) ~= xsize then
                    schema_error(ss, 'Bad fixed type size: %s', xsize)
                end
                res.size = xsize
                remove(ss)
                return res
            else
                schema_error(ss, 'Unknown Avro "type": %s', xtype)
            end
        end
    else
        local typeid = tostring(schema)
        if primitive_type[typeid] then
            return { type = typeid }
        end
        typeid = fullname(typeid, ss)
        schema = scope[typeid]
        if schema then
            return schema
        end
        schema_error(ss, 'Unknown Avro "type": %s', typeid) 
    end
end

local function create_schema(schema)
    local scope = {}
    local root = create_schema1(schema, {}, scope)
    -- XXX process defaults
    return root
end

return {
    schema_error = schema_error,
    validname = validname,
    fullname = fullname,
    validfullname = validfullname,
    checkname = checkname,
    checkaliases = checkaliases,
    create_schema = create_schema
}
