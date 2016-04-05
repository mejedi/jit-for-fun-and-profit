return {
    type = 'record',
    name = 'Person',
    namespace = 'Person',
    fields = {
        { name = 'FirstName', type = 'string' },
        { name = 'LastName',  type = 'string' },
        { name = 'Class',     type = 'string' },
        { name = 'Age',       type = 'long'   },
        { 
            name = 'Sex',
            type = {
                type = 'enum',
                name = 'Sex',
                symbols = { 'FEMALE', 'MALE' }
            }
        },
        {
            name = 'Stats',
            type = {
                type = 'record',
                name = 'Stats',
                fields = {
                    { name = 'Strength',     type = 'long' },
                    { name = 'Perception',   type = 'long' },
                    { name = 'Endurance',    type = 'long' },
                    { name = 'Charisma',     type = 'long' },
                    { name = 'Intelligence', type = 'long' },
                    { name = 'Agility',      type = 'long' },
                    { name = 'Luck',         type = 'long' }
                }
            }
        },
        {
            name = 'Journal',
            type = {
                type  = 'array',
                items = 'string'
            }
        }
    }
}
