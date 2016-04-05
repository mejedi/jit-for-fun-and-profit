local ffi         = require('ffi')
local digest      = require('digest')
local clock       = require('clock')
local schema_util = require('schema_util')

local schema_util_C = schema_util.schema_util_C

local null = ffi.cast('void *', 0)
local regs = ffi.new('struct tarantool_schema_proc_Regs')

local bank = digest.base64_decode('p0pvdXJuYWykTHVja6dBZ2lsaXR5rEludGVsbGlnZW5jZahDaGFyaXNtYalFbmR1cmFuY2WqUGVyY2VwdGlvbqhTdHJlbmd0aKVTdGF0c6RNQUxFpkZFTUFMRaNTZXijQWdlpUNsYXNzqExhc3ROYW1lqUZpcnN0TmFtZQ==')

local function unknown_key(ks, kl)
    error('unknown key: '..ffi.string(ks, kl))
end

local function flatten(data)

    local r = regs

    r.b1 = ffi.cast('const uint8_t *', data)
    r.b1 = r.b1 + #data
    r.b2 = ffi.cast('const uint8_t *', bank)
    r.b2 = r.b2 + #bank

    r.rc = schema_util_C.preprocess_msgpack(data, #data, 0, null, null, r.t, r.v)
    if r.rc < 0 then
        error('preprocess_msgpack: -1')
    end

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

    if r.t[0][0] ~= 12 then
        error('::root:: not map')
    end

    --
    -- Process keys
    --
    local i, stop = 1, r.v[0][0].xoff
    while i ~= stop do
        if r.t[0][i] ~= 8 then error('::root:: key not str') end
        r.ks, r.kl = r.b1 - r.v[0][i].xoff, r.v[0][i].xlen
        if r.kl < 3 then unknown_key(r.ks, r.kl) end
        if     r.ks[1] == 105 then -- F_i_rstName
            if r.kl ~= 9 or ffi.C.memcmp(r.ks, r.b2 - 9, 9) ~= 0 then
                unknown_key(ks, kl)
            end
            if r.t[0][i+1] ~= 8 then error('FirstName not str') end
            if rr0 ~= 0 then error('FirstName dup') end
            rr0 = i + 1
            i = i + 2
        elseif r.ks[1] ==  97 then -- L_a_stName
            if r.kl ~= 8 or ffi.C.memcmp(r.ks, r.b2 - 18, 8) ~= 0 then
                unknown_key(r.ks, r.kl)
            end
            if r.t[0][i+1] ~= 8 then error('LastName not str') end
            if rr1 ~= 0 then error('LastName dup') end
            rr1 = i + 1
            i = i + 2
        elseif r.ks[1] == 108 then -- C_l_ass
            if r.kl ~= 5 or ffi.C.memcmp(r.ks, r.b2 - 24, 5) ~= 0 then
                unknown_key(r.ks, r.kl)
            end
            if r.t[0][i+1] ~= 8 then error('Class not str') end
            if rr2 ~= 0 then error('Class dup') end
            rr2 = i + 1
            i = i + 2
        elseif r.ks[1] == 103 then -- A_g_e
            if r.kl ~= 3 or ffi.C.memcmp(r.ks, r.b2 - 28, 3) ~= 0 then
                unknown_key(r.ks, r.kl)
            end
            if r.t[0][i+1] ~= 4 then error('Age not long') end
            if rr3 ~= 0 then error('Age dup') end
            rr3 = i + 1
            i = i + 2
        elseif r.ks[1] == 101 then -- S_e_x
            if r.kl ~= 3 or ffi.C.memcmp(r.ks, r.b2 - 32, 3) ~= 0 then
                unknown_key(r.ks, r.kl)
            end
            if r.t[0][i+1] ~= 8 then error('Sex not str') end
            if rr4 ~= 2 then error('Sex dup') end
            r.ks, r.kl = r.b1 - r.v[0][i+1].xoff, r.v[0][i+1].xlen
            if r.kl < 4 then error('wrong Sex') end
            if     r.ks[0] == 70 then -- F_EMALE
                if r.kl ~= 6 or ffi.C.memcmp(r.ks, r.b2 - 39, 6) ~= 0 then
                    error('wrong Sex')
                end
                rr4 = 0
            elseif r.ks[0] == 77 then -- M_ALE
                if r.kl ~= 4 or ffi.C.memcmp(r.ks, r.b2 - 44, 4) ~= 0 then
                    error('wrong Sex')
                end
                rr4 = 1
            else
                error('wrong Sex ')
            end
            i = i + 2
        elseif r.ks[1] == 116 then -- S_t_ats
            if r.kl ~= 5 or ffi.C.memcmp(r.ks, r.b2 - 50, 5) ~= 0 then
                unknown_key(r.ks, r.kl)
            end
            if r.t[0][i+1] ~= 12 then error('Stats not map') end
            if rr5 ~= 0 then error('Stats dup') end
            rr5 = i + 1
            local stop = rr5 + r.v[0][rr5].xoff
            i = i + 2
            while i ~= stop do
                if r.t[0][i] ~= 8 then error('Stats key not str') end
                r.ks, r.kl = r.b1 - r.v[0][i].xoff, r.v[0][i].xlen
                if r.kl < 4 then unknown_key(r.ks, r.kl) end
                if     r.ks[0] == 83 then -- S_trength
                    if r.kl ~= 8 or ffi.C.memcmp(r.ks, r.b2 - 59, 8) ~= 0 then
                        unknown_key(r.ks, r.kl)
                    end
                    if rr6 ~= 0 then error('Stats.Strength dup') end
                    rr6 = i + 1
                elseif r.ks[0] == 80 then -- P_erception
                    if r.kl ~= 10 or ffi.C.memcmp(r.ks, r.b2 - 70, 10) ~= 0 then
                        unknown_key(r.ks, r.kl)
                    end
                    if rr7 ~= 0 then error('Stats.Perception dup') end
                    rr7 = i + 1
                elseif r.ks[0] == 69 then -- E_ndurance
                    if r.kl ~= 9 or ffi.C.memcmp(r.ks, r.b2 - 80, 9) ~= 0 then
                        unknown_key(r.ks, r.kl)
                    end
                    if rr8 ~= 0 then error('Stats.Endurance dup') end
                    rr8 = i + 1
                elseif r.ks[0] == 67 then -- C_harisma
                    if r.kl ~= 8 or ffi.C.memcmp(r.ks, r.b2 - 89, 8) ~= 0 then
                        unknown_key(r.ks, r.kl)
                    end
                    if rr9 ~= 0 then error('Stats.Charisma dup') end
                    rr9 = i + 1
                elseif r.ks[0] == 73 then -- I_ntelligence
                    if r.kl ~= 12 or ffi.C.memcmp(r.ks, r.b2 - 102, 12) ~= 0 then
                        unknown_key(r.ks, r.kl)
                    end
                    if rr10 ~= 0 then error('Stats.Intelligence dup') end
                    rr10 = i + 1
                elseif r.ks[0] == 65 then -- A_gility
                    if r.kl ~= 7 or ffi.C.memcmp(r.ks, r.b2 - 110, 7) ~= 0 then
                        unknown_key(r.ks, r.kl)
                    end
                    if rr11 ~= 0 then error('Stats.Agility dup') end
                    rr11 = i + 1
                elseif r.ks[0] == 76 then -- L_uck
                    if r.kl ~= 4 or ffi.C.memcmp(r.ks, r.b2 - 115, 4) ~= 0 then
                        unknown_key(r.ks, r.kl)
                    end
                    if rr12 ~= 0 then error('Stats.Luck dup') end
                    rr12 = i + 1
                else
                    unknown_key(r.ks, r.kl)
                end
                if r.t[0][i+1] ~= 4 then error('Stats.'..ffi.string(r.ks, r.kl)..' not long') end
                i = i + 2
            end
        elseif r.ks[1] == 111 then -- J_o_urnal ]]
            if r.kl ~= 7 or ffi.C.memcmp(r.ks, r.b2 - 123, 7) ~= 0 then
                unknown_key(r.ks, r.kl)
            end
            if r.t[0][i+1] ~= 11 then error('Journal not array') end
            if rr13 ~= 0 then error('Journal dup') end
            rr13 = i + 1
            i = rr13 + r.v[0][rr13].xoff
        else
            unknown_key(r.ks, r.kl)
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
    local slots = 14 + r.v[0][rr13].xlen
    r.ot = ffi.C.malloc(slots)
    r.ov = ffi.C.malloc(slots * 8)
    if r.ot == 0 or r.ov == 0 then
        error('malloc')
    end

    r.ot[0 ] = 11; r.ov[0 ].xlen = 13
    -- #1  FirstName
    r.ot[1 ] =  8; r.ov[1 ] = r.v[0][rr0]
    -- #2  LastName
    r.ot[2 ] =  8; r.ov[2 ] = r.v[0][rr1]
    -- #3  Class
    r.ot[3 ] =  8; r.ov[3 ] = r.v[0][rr2]
    -- #4  Age
    r.ot[4 ] =  4; r.ov[4 ] = r.v[0][rr3]
    -- #5  Sex
    r.ot[5 ] =  4; r.ov[5 ].uval = rr4
    -- #6  Stats.Strength
    r.ot[6 ] =  4; r.ov[6 ] = r.v[0][rr6]
    -- #7  Stats.Perception
    r.ot[7 ] =  4; r.ov[7 ] = r.v[0][rr7]
    -- #8  Stats.Endurance
    r.ot[8 ] =  4; r.ov[8 ] = r.v[0][rr8]
    -- #9  Stats.Charisma
    r.ot[9 ] =  4; r.ov[9 ] = r.v[0][rr9]
    -- #10 Stats.Intelligence
    r.ot[10] =  4; r.ov[10] = r.v[0][rr10]
    -- #11 Stats.Agility
    r.ot[11] =  4; r.ov[11] = r.v[0][rr11]
    -- #12 Stats.Luck
    r.ot[12] =  4; r.ov[12] = r.v[0][rr12]
    -- #13 Journal
    r.ot[13] = 11; r.ov[13].xlen = r.v[0][rr13].xlen

    local i, n = 0, r.v[0][rr13].xlen
    while i ~= n do
        if r.t[0][rr13 + i + 1] ~= 8 then
            error(string.format('Journal[%d] not str', i))
        end
        r.ot[14 + i] = 8; r.ov[14 + i] = r.v[0][rr13 + i + 1]
        i = i + 1
    end

    r.rc = schema_util_C.create_msgpack(slots, r.ot, r.ov, r.b1, r.b2, 0, null, r.res)
    if r.rc < 0 then
        error('create_msgpack: -1')
    end
    local res = ffi.string(r.res[0], r.rc)

    ffi.C.free(r.t[0])
    ffi.C.free(r.v[0])
    ffi.C.free(r.ot)
    ffi.C.free(r.ov)
    ffi.C.free(r.res[0])

    return res
end

return {
    flatten = flatten,
    benchmark = function(n)
        local data = require('john').john_msgpack
        local expected = digest.base64_decode('naRKb2huo0RvZapUZWNoV2l6YXJkEQEDBQEECQMGltlEWW91IGFyZSBzdGFuZGluZyBhdCB0aGUgZW5kIG9mIGEgcm9hZCBiZWZvcmUgYSBzbWFsbCBicmljayBidWlsZGluZy63QXJvdW5kIHlvdSBpcyBhIGZvcmVzdC7ZOkEgc21hbGwgc3RyZWFtIHBsb3dzIG91dCBvZiB0aGUgYnVpbGRpbmcgYW5kIGRvd24gYSBndWxseS61WW91IGVudGVyIHRoZSBmb3Jlc3Qu2U1Zb3UgYXJlIGluIGEgdmFsbGV5IGluIHRoZSBmb3Jlc3QgYmVzaWRlcyBhIHN0cmVhbSB0dW1saW5nIGFsb25nIGEgcm9ja3kgZW5kLrFZb3UgZmVlbCB0aGlyc3R5IQ==')
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

