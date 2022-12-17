import asyncio

import requests
from aiohttp import ClientSession
from db import engine, Base, Session, Character
from more_itertools import chunked

CHUNK_SIZE = 10


async def get_pers(pers_id):
    session = ClientSession()
    response = await session.get(f'https://swapi.dev/api/people/{pers_id}')
    person = await response.json()
    await session.close()
    return person


async def get_character(start, end):
    for id_chunk in chunked(range(start, end), CHUNK_SIZE):
        coroutines = [get_pers(i) for i in id_chunk]
        pers = await asyncio.gather(*coroutines)
        for character in pers:
            yield character


async def paste_db(all_datas):
    async with Session() as session:

        char_orm = [Character(
            birth_year=character['birth_year'],
            eye_color=character['birth_year'],
            films=[requests.get(film).json()['title'] for film in character['films']],
            gender=character['gender'],
            hair_color=character['hair_color'],
            height=character['height'],
            homeworld=character['homeworld'],
            mass=character['mass'],
            name=character['name'],
            skin_color=character['skin_color'],
            species=[requests.get(specie).json()['name'] for specie in character['species']],
            starships=[requests.get(starship).json()['name'] for starship in character['starships']],
            vehicles=[requests.get(vehicle).json()['name'] for vehicle in character['vehicles']],
        ) for character in all_datas if 'detail' not in character]
        session.add_all(char_orm)
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
    buffer_data = []
    end = requests.get('https://swapi.dev/api/people/').json()['count']
    async for character in get_character(1, int(end)+2):
        print(type(character), character)
        buffer_data.append(character)
        if len(buffer_data) >= 10:
            asyncio.create_task(paste_db(buffer_data))
            buffer_data = []

    if buffer_data:
        await paste_db(buffer_data)

    await engine.dispose()


asyncio.run(main())
