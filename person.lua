local ffi         = require('ffi')
local digest      = require('digest')
local clock       = require('clock')
local schema_util = require('schema_util')

local schema_util_C = schema_util.schema_util_C

local null = ffi.cast('void *', 0)
local out_arg1 = ffi.new('uint8_t *[1]')
local out_arg2 = ffi.new('struct tarantool_schema_preproc_Value *[1]')

local function preprocess_msgpack(msgpack)
    local rc = schema_util_C.preprocess_msgpack(
        msgpack, #msgpack, 0, null, null, out_arg1, out_arg2)
    if rc < 0 then
        error('schema_util_C.preprocess_msgpack: -1')
    end
    return rc, out_arg1[0], out_arg2[0]
end
    
local function create_msgpack(n, t, v, b1, b2)
    local rc = schema_util_C.create_msgpack(
        n, t, v, b1, b2, 0, null, out_arg1)
    if rc < 0 then
        error('schema_util_C.create_msgpack: -1')
    end
    return rc, out_arg1[0], ffi.C.free
end

local bank = digest.base64_decode('p0pvdXJuYWykTHVja6dBZ2lsaXR5rEludGVsbGlnZW5jZahDaGFyaXNtYalFbmR1cmFuY2WqUGVyY2VwdGlvbqhTdHJlbmd0aKVTdGF0c6RNQUxFpkZFTUFMRaNTZXijQWdlpUNsYXNzqExhc3ROYW1lqUZpcnN0TmFtZQ==')

local function unknown_key(ks, kl)
    error('unknown key: '..ffi.string(ks, kl))
end

local function flatten(input)

    local n, t, v = preprocess_msgpack(input)
    local b = ffi.cast('const uint8_t *', bank) + #bank
    local ib = ffi.cast('const uint8_t *', input) + #input

    local rr0  = 0 -- FirstName
    local rr1  = 0 -- LastName
    local rr2  = 0 -- Class
    local rr3  = 0 -- Age
    local rr4  = 2 -- Sex
    local rr5  = 0 -- Stats
    local rr6  = 0 -- Stats.Strength
    local rr7  = 0 -- Stats.Perception
    local rr8  = 0 -- Stats.Endurance
    local rr9  = 0 -- Stats.Charisma
    local rr10 = 0 -- Stats.Intelligence
    local rr11 = 0 -- Stats.Agility
    local rr12 = 0 -- Stats.Luck
    local rr13 = 0 -- Stats.Journal
    
    if t[0] ~= 12 then
        error('::root:: not map')
    end

    --
    -- Process keys
    --
    local i, stop = 1, v[0].xoff
    while i ~= stop do
        if t[i] ~= 8 then error('::root:: key not str') end
        local ks, kl = ib - v[i].xoff, v[i].xlen
        if kl < 3 then unknown_key(ks, kl) end
        if     ks[1] == 105 then -- F_i_rstName
            if kl ~= 9 or ffi.C.memcmp(ks, b - 9, 9) ~= 0 then
                unknown_key(ks, kl)
            end
            if t[i+1] ~= 8 then error('FirstName not str') end
            if rr0 ~= 0 then error('FirstName dup') end
            rr0 = i + 1
            i = i + 2
        elseif ks[1] ==  97 then -- L_a_stName
            if kl ~= 8 or ffi.C.memcmp(ks, b - 18, 8) ~= 0 then
                unknown_key(ks, kl)
            end
            if t[i+1] ~= 8 then error('LastName not str') end
            if rr1 ~= 0 then error('LastName dup') end
            rr1 = i + 1
            i = i + 2
        elseif ks[1] == 108 then -- C_l_ass
            if kl ~= 5 or ffi.C.memcmp(ks, b - 24, 5) ~= 0 then
                unknown_key(ks, kl)
            end
            if t[i+1] ~= 8 then error('Class not str') end
            if rr2 ~= 0 then error('Class dup') end
            rr2 = i + 1
            i = i + 2
        elseif ks[1] == 103 then -- A_g_e
            if kl ~= 3 or ffi.C.memcmp(ks, b - 28, 3) ~= 0 then
                unknown_key(ks, kl)
            end
            if t[i+1] ~= 4 then error('Age not long') end
            if rr3 ~= 0 then error('Age dup') end
            rr3 = i + 1
            i = i + 2
        elseif ks[1] == 101 then -- S_e_x
            if kl ~= 3 or ffi.C.memcmp(ks, b - 32, 3) ~= 0 then
                unknown_key(ks, kl)
            end
            if t[i+1] ~= 8 then error('Sex not str') end
            if rr4 ~= 2 then error('Sex dup') end
            local ks, kl = ib - v[i+1].xoff, v[i+1].xlen
            if kl < 4 then error('wrong Sex') end
            if     ks[0] == 70 then -- F_EMALE
                if kl ~= 6 or ffi.C.memcmp(ks, b - 39, 6) ~= 0 then
                    error('wrong Sex')
                end
                rr4 = 0
            elseif ks[0] == 77 then -- M_ALE
                if kl ~= 4 or ffi.C.memcmp(ks, b - 44, 4) ~= 0 then
                    error('wrong Sex')
                end
                rr4 = 1
            else
                error('wrong Sex')
            end
            rr4 = i + 1
            i = i + 2
        elseif ks[1] == 116 then -- S_t_ats
            if kl ~= 5 or ffi.C.memcmp(ks, b - 50, 5) ~= 0 then
                unknown_key(ks, kl)
            end
            if t[i+1] ~= 12 then error('Stats not map') end
            if rr5 ~= 0 then error('Stats dup') end
            rr5 = i + 1
            local stop = rr5 + v[rr5].xoff
            i = i + 2
            while i ~= stop do
                if t[i] ~= 8 then error('Stats key not str') end
                local ks, kl = ib - v[i].xoff, v[i].xlen
                if kl < 4 then unknown_key(ks, kl) end
                if     ks[0] == 83 then -- S_trength
                    if kl ~= 8 or ffi.C.memcmp(ks, b - 59, 8) ~= 0 then
                        unknown_key(ks, kl)
                    end
                    if rr6 ~= 0 then error('Stats.Strength dup') end
                    rr6 = i + 1
                elseif ks[0] == 80 then -- P_erception
                    if kl ~= 10 or ffi.C.memcmp(ks, b - 70, 10) ~= 0 then
                        unknown_key(ks, kl)
                    end
                    if rr7 ~= 0 then error('Stats.Perception dup') end
                    rr7 = i + 1
                elseif ks[0] == 69 then -- E_ndurance
                    if kl ~= 9 or ffi.C.memcmp(ks, b - 80, 9) ~= 0 then
                        unknown_key(ks, kl)
                    end
                    if rr8 ~= 0 then error('Stats.Endurance dup') end
                    rr8 = i + 1
                elseif ks[0] == 67 then -- C_harisma
                    if kl ~= 8 or ffi.C.memcmp(ks, b - 89, 8) ~= 0 then
                        unknown_key(ks, kl)
                    end
                    if rr9 ~= 0 then error('Stats.Charisma dup') end
                    rr9 = i + 1
                elseif ks[0] == 73 then -- I_ntelligence
                    if kl ~= 12 or ffi.C.memcmp(ks, b - 102, 12) ~= 0 then
                        unknown_key(ks, kl)
                    end
                    if rr10 ~= 0 then error('Stats.Intelligence dup') end
                    rr10 = i + 1
                elseif ks[0] == 65 then -- A_gility
                    if kl ~= 7 or ffi.C.memcmp(ks, b - 110, 7) ~= 0 then
                        unknown_key(ks, kl)
                    end
                    if rr11 ~= 0 then error('Stats.Agility dup') end
                    rr11 = i + 1
                elseif ks[0] == 76 then -- L_uck
                    if kl ~= 4 or ffi.C.memcmp(ks, b - 115, 4) ~= 0 then
                        unknown_key(ks, kl)
                    end
                    if rr12 ~= 0 then error('Stats.Luck dup') end
                    rr12 = i + 1
                else
                    unknown_key(ks, kl)
                end
                if t[i+1] ~= 4 then error('Stats.'..ffi.string(ks, kl)..' not long') end
                i = i + 2
            end
        elseif ks[1] == 111 then -- J_o_urnal ]]
            if kl ~= 7 or ffi.C.memcmp(ks, b - 123, 7) ~= 0 then
                unknown_key(ks, kl)
            end
            if t[i+1] ~= 11 then error('Journal not array') end
            if rr13 ~= 0 then error('Journal dup') end
            rr13 = i + 1
            i = rr13 + v[rr13].xoff
        else
            unknown_key(ks, kl)
        end
    end
    if rr0  == 0 then error('FirstName missing') end
    if rr1  == 0 then error('LastName missing') end
    if rr2  == 0 then error('Class missing') end
    if rr3  == 0 then error('Age missing') end
    if rr4  == 2 then error('Sex missing') end
    if rr5  == 0 then error('Stats missing') end
    if rr6  == 0 then error('Stats.Strength missing') end
    if rr7  == 0 then error('Stats.Perception missing') end
    if rr8  == 0 then error('Stats.Endurance missing') end
    if rr9  == 0 then error('Stats.Charisma missing') end
    if rr10 == 0 then error('Stats.Intelligence missing') end
    if rr11 == 0 then error('Stats.Agility missing') end
    if rr12 == 0 then error('Stats.Luck missing') end
    if rr13 == 0 then error('Journal missing') end

    --
    -- begin output stage 
    --

    -- reserve 14 + journal.length slots
    local slots = 14 + v[rr13].xlen
    local ot = ffi.cast('uint8_t *', ffi.C.malloc(slots))
    local ov = ffi.cast('struct tarantool_schema_preproc_Value *', ffi.C.malloc(slots * 8))
    if ot == 0 or ov == 0 then
        error('malloc')
    end

    ot[0 ] = 11; ov[0 ].xlen = 13
    -- #1  FirstName
    ot[1 ] =  8; ov[1 ] = v[rr0]
    -- #2  LastName
    ot[2 ] =  8; ov[2 ] = v[rr1]
    -- #3  Class
    ot[3 ] =  8; ov[3 ] = v[rr2]
    -- #4  Age
    ot[4 ] =  4; ov[4 ] = v[rr3]
    -- #5  Sex
    ot[5 ] =  4; ov[5 ].uval = rr4
    -- #6  Stats.Strength
    ot[6 ] =  4; ov[6 ] = v[rr6]
    -- #7  Stats.Perception
    ot[7 ] =  4; ov[7 ] = v[rr7]
    -- #8  Stats.Endurance
    ot[8 ] =  4; ov[8 ] = v[rr8]
    -- #9  Stats.Charisma
    ot[9 ] =  4; ov[9 ] = v[rr9]
    -- #10 Stats.Intelligence
    ot[10] =  4; ov[10] = v[rr10]
    -- #11 Stats.Agility
    ot[11] =  4; ov[11] = v[rr11]
    -- #12 Stats.Luck
    ot[12] =  4; ov[12] = v[rr12]
    -- #13 Journal
    ot[13] = 11; ov[13].xlen = v[rr13].xlen

    local i, n = 0, v[rr13].xlen
    while i ~= n do
        if t[rr13 + i + 1] ~= 8 then
            error(string.format('Journal[%d] not str', i))
        end
        ot[14 + i] = 8; ov[14 + i] = v[rr13 + i + 1]
        i = i + 1
    end

    local len, data = create_msgpack(slots, ot, ov, ib, b)
    local res = ffi.string(data, len)

    ffi.C.free(t)
    ffi.C.free(v)
    ffi.C.free(ot)
    ffi.C.free(ov)
    ffi.C.free(data)

    return res
end

return {
    flatten = flatten,
    benchmark = function(n)
        local data = require('john').john_msgpack
        local expected = digest.base64_decode('naRKb2huo0RvZapUZWNoV2l6YXJkESADBQEECQMGltlEWW91IGFyZSBzdGFuZGluZyBhdCB0aGUgZW5kIG9mIGEgcm9hZCBiZWZvcmUgYSBzbWFsbCBicmljayBidWlsZGluZy63QXJvdW5kIHlvdSBpcyBhIGZvcmVzdC7ZOkEgc21hbGwgc3RyZWFtIHBsb3dzIG91dCBvZiB0aGUgYnVpbGRpbmcgYW5kIGRvd24gYSBndWxseS61WW91IGVudGVyIHRoZSBmb3Jlc3Qu2U1Zb3UgYXJlIGluIGEgdmFsbGV5IGluIHRoZSBmb3Jlc3QgYmVzaWRlcyBhIHN0cmVhbSB0dW1saW5nIGFsb25nIGEgcm9ja3kgZW5kLrFZb3UgZmVlbCB0aGlyc3R5IQ==')
        if expected ~= flatten(data) then
            error('sanity check')
        end
        n = n or 1000000
        local t = clock.bench(function()
            for i = 1,n do
                flatten(data)   
            end
        end)[1]
        print(string.format('RPS: %d', math.floor(n/t)))
    end
}

