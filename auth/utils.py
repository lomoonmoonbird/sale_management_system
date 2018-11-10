from motor.core import Collection


async def insert_area(col: Collection, name: str):
    global_id = str((await col.find_one({'role': 1}))['_id'])
    return await col.insert_one({'name': name, 'role': 2, 'parent_id': global_id})


async def insert_channel(col: Collection, name: str, parent_id: str, old_id: int):
    await col.insert_one({'name': name, 'role': 3, 'parent_id': parent_id, 'old_id': old_id})


async def insert_market(col: Collection, name: str, parent_id: str):
    await col.insert_one({'name': name, 'role': 4, 'parent_id': parent_id})


async def insert_school(col: Collection, name: str, parent_id: str, old_id: int):
    await col.insert_one({'name': name, 'role': 5, 'parent_id': parent_id, 'old_id': old_id})


async def find_area(col: Collection, max_length=10000) -> list:
    cursor = col.find({'role': 2})
    return await cursor.to_list(max_length)


async def find_channel(col: Collection, parent_id=None, max_length=10000) -> list:
    if parent_id:
        cursor = col.find({'role': 3, 'parent_id': parent_id})
    else:
        cursor = col.find({'role': 3})
    return await cursor.to_list(max_length)


async def find_market(col: Collection, parent_id=None, max_length=10000) -> list:
    if parent_id:
        cursor = col.find({'role': 4, 'parent_id': parent_id})
    else:
        cursor = col.find({'role': 4})
    return await cursor.to_list(max_length)


async def find_school(col: Collection, parent_id=None, max_length=10000) -> list:
    if parent_id:
        cursor = col.find({'role': 5, 'parent_id': parent_id})
    else:
        cursor = col.find({'role': 5})
    return await cursor.to_list(max_length)
