
-- Count the particular IR block uses.
-- We need it to decide whether to inline the particular block. 
-- A block could be used to either generate a conversion code (counters_c)
-- or to generate validation code (counters_v).
-- The later is done when a field is missing in target schema.
local function count_refs(ir, counters_c, counters_v)
    if type(ir) == 'string' then
        return
    end
    local count = counters_c[ir]
    if count then
        counters_c[ir] = count + 1
        return
    end
    counters_c[ir] = 1
    if      ir[1] == 'ARRAY' or or[1] == 'MAP' then
        count_refs(ir[2], counters_c, counters_v)
    else if ir[1] == 'UNION' or ir[1] == 'RECORD' then
        local bc = ir[2]
        local mm = ir[4]
        for i = 1, #bc do
            if mm[i] then
                count_refs(bc[i], counters_c, counters_v)
            elseif or[1] ~= 'UNION' then
                -- in union it is a runtime error
                count_refs(bc[i], counters_v, counters_v)
            end
        end
    end
end

--
-- 
--
--

local function variables_tracker(scope, prefix)
    return {
        scope = scope,
        prefix = prefix or 'v',
        c = 1
    }
end

-- declare new variable
local function make_variable(vt, val)
    local res
    local stash = vt.stash
    if stash then
        res = remove(stash)
        if res then
            return res, val and format('%s = %s', res, val) or nil
        end
    end
    local c = vt.c
    res = format('%s%03d', vt.prefix, c)
    vt.c = c + 1
    if val then
        insert(vt.scope, format('local %s = %s', res, val))
    else
        insert(vt.scope, format('local %s', res))
    end
    return res
end

-- the variable is no longer used (it may be reused by a sebsequent make_variable)
local function kill_variable(vt, var)
    local stash = vt.stash
    if not stash then
        stash = {}
        vt.stash = stash
    end
    insert(stash, var)
end

--
local function fn()

end

local function generate_code(ir)

    local preamble = {
        'local ffi       = require("ffi")',
        'local digest    = require("digest")',
        'local bit       = require("bit")',
        'local schema_rt = require("schema_util")',
        '',
        'local ffi_C     = ffi.C',
        'local ffi_cast  = ffi.cast',
        ''
    }

    local functions = {
    }

    local func_tracker = variables_tracker(functions, 'func')

    local code = {
        preamble,
        functions
    }

    flatten = {
        'local function flatten (data)',
        type = 'func'
    }

    insert(code, flatten)

    local locals = {
        'local r = regs',
        'local res'
    }

    insert(flatten, locals)


    insert(flatten, 'end')
end

