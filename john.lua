local john  = {
    FirstName = 'John',
    LastName  = 'Doe',
    Class     = 'TechWizard',
    Age       = 17,
    Sex       = 'MALE',
    Stats     = {
        Strength     = 3,
        Perception   = 5,
        Endurance    = 1,
        Charisma     = 4,
        Intelligence = 9,
        Agility      = 3,
        Luck         = 6
    },
    Journal   = {
        'You are standing at the end of a road before a small brick building.',
        'Around you is a forest.',
        'A small stream plows out of the building and down a gully.',
        'You enter the forest.',
        'You are in a valley in the forest besides a stream tumling along a rocky end.',
        'You feel thirsty!'
    }
}

return {
    john = john,
    john_msgpack = require('msgpack').encode(john)
}
