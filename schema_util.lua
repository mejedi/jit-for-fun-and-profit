local ffi = require('ffi')

local find, format = string.find, string.format
local byte, sub = string.byte, string.sub
local concat, insert = table.concat, table.insert
local remove = table.remove

ffi.cdef[[

struct tarantool_schema_preproc_Value {
    union {
        int64_t        ival;
        uint64_t       uval;
        double         dval;
        struct {
            uint32_t   xlen;
            uint32_t   xoff;
        };
    };
};

struct tarantool_schema_proc_Regs {
    ssize_t                   rc;
    uint8_t                  *t[1];
    struct tarantool_schema_preproc_Value
                             *v[1];
    const uint8_t            *b1;
    const uint8_t            *b2;
    uint8_t                  *ot;
    struct tarantool_schema_preproc_Value
                             *ov;
    uint8_t                  *res[1];
    const uint8_t            *ks;
    size_t                    kl;
};

ssize_t
preprocess_msgpack(const uint8_t *msgpack_in,
                   size_t         msgpack_size,
                   size_t         stock_buf_size_or_hint,
                   uint8_t       *stock_typeid_buf,
                   struct tarantool_schema_preproc_Value
                                 *stock_value_buf,
                   uint8_t      **typeid_out,
                   struct tarantool_schema_preproc_Value
                                **value_out);

ssize_t
create_msgpack(size_t             nitems,
               const uint8_t     *typeid,
               const struct tarantool_schema_preproc_Value
                                 *value,
               const uint8_t     *bank1,
               const uint8_t     *bank2,
               size_t             stock_buf_size_or_hint,
               uint8_t           *stock_buf,
               uint8_t          **msgpack_out);

void *malloc(size_t);
void  free(void *);
int   memcmp(const void *, const void *, size_t);

]]

local null = ffi.cast('void *', 0)
local schema_util_C = ffi.load('./schema_util.so')

--
-- visualize_msgpack
--

local function esc(s)
    if find(s, '[A-Za-z0-9_]') then
        return s
    else
        return format('\\%0d', string.byte(s))
    end
end

local typenames = {
    [1] = 'NIL', [2] = 'FALSE', [3] = 'TRUE', [4] = 'LONG', [5] = 'ULONG',
    [6] = 'FLOAT', [7] = 'DOUBLE', [8] = 'STR', [9] = 'BIN',
    [10] = 'EXT', [11] = 'ARRAY', [12] = 'MAP'
}

local valuevis = {
    [4] = function(i, val)
        return nil, format(' %4s', val.ival)
    end,
    [5] = function(i, val)
        return nil, format(' %4s', val.uval)
    end,
    [6] = function(i,val)
        return nil, format(' %4s', val.dval)
    end,
    [7] = function(i, val)
        return nil, format(' %4s', val.dval)
    end,
    [8] = function(i, val, bank)
        local sample
        if type(bank) == 'string' then
            sample = {}
            local i = #bank - val.xoff + 1
            while #sample < val.xlen do
                if #sample == 10 then
                    sample[8], sample[9], sample[10] = '.', '.', '.'
                    break
                end
                insert(sample, esc(sub(bank, i, i)))
                i = i + 1
            end
            sample = concat(sample)
        else
            sample = format('-%d', val.xoff)
        end
        return nil, format(' %4s %s', val.xlen, sample or '')
    end,
    [11] = function(i, val)
        return val.xlen, format(
            ' %4s ->%05d', val.xlen, i+val.xoff)
    end,
    [12] = function(i, val)
        return val.xlen * 2, format(
            ' %4s ->%05d', val.xlen, i+val.xoff)
    end
}

local function visualize_msgpack(input)
    local typeid_out = ffi.new('uint8_t *[1]');
    local value_out  = ffi.new('struct tarantool_schema_preproc_Value *[1]')
    
    local rc = schema_util_C.preprocess_msgpack(
        input, #input, 0, null, null, typeid_out, value_out)

    if rc < 0 then
        error('schema_util_C.preprocess_msgpack: -1')
    end

    local st, res = pcall(function()

        local typeid = typeid_out[0]
        local value  = value_out[0]

        local output = {}
        local todos = {}
        local todo = 1
        for i = 0, tonumber(rc) - 1 do
            local indent
            local xid, xval = typeid[i], value[i]
            local vis = valuevis[xid]
            local len, info
            if vis then
                len, info = vis(i, xval, input)
            end
            local line = format(
                '%05d%s %-06s%s', i, string.rep('....', #todos),
                typenames[xid] or '???', info or '')
            insert(output, line)
            todo = todo - 1
            if len then
                insert(todos, todo)
                todo = len
            end
            while todo == 0 and todos[1] do
                todo = remove(todos)
            end
        end

        insert(output, format('%05d', tonumber(rc)))
        return concat(output, '\n')

    end)

    ffi.C.free(typeid_out[0])
    ffi.C.free(value_out[0])
    
    if not st then
        error(res)
    end
    return res
end

return {
    visualize_msgpack = visualize_msgpack,
    schema_util_C = schema_util_C
}
