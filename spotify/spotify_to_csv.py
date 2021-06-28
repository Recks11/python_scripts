import pandas as pd
import numpy as np

FIELDS = ['Track Number', 'Track Name', 'Album Artist Name(s)', 'Album Name', 'Artist Genres', 'Track Duration (ms)',
          'Album Release Date']
COLUMNS = ['number', 'name', 'artist', 'album', 'genres', 'duration', 'released']

data = pd.read_csv('../data/music/afro_pop.csv', index_col=0) \
    .append(pd.read_csv('../data/music/daniel_caesar.csv', index_col=0)) \
    .append(pd.read_csv('../data/music/rythm_and_poetry.csv', index_col=0)) \
    .append(pd.read_csv('../data/music/the_nbhd.csv', index_col=0)) \
    .append(pd.read_csv('../data/music/RB.csv', index_col=0))
data['Album Release Date'] = pd.to_datetime(data['Album Release Date'])

music = data[FIELDS]
random = np.random.randint(len(music), size=200)
music.index = range(len(music))
music.columns = COLUMNS

music.to_csv(r'data/music_data.csv', index=False, header=True)


def spl(n):
    if type(n) == str:
        return n.split(',')
    return []


music = pd.read_csv('../data/music_data.csv')
music_json = music.copy()
music_json['genres'] = music['genres'].apply(spl)
music_json = music_json.drop(columns=['duration', 'released'])
music_json.to_json(r'data/music_data.json', orient='records')

lst = pd.read_json('../data/listens_data.json')
lst['n_listens'] = lst['listen_duration'] / int(60000)
lst['n_listens'] = lst['n_listens'].round().astype(int)

lst.to_json(r'data/lst_data.json', orient='records')