import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import numpy as np
import sqlite3
import base64


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
image_filename = "calf1.png" # replace with your own image
encoded_image = base64.b64encode(open(image_filename, 'rb').read())

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
insee_col =['siren',
 'siret',
 'dateCreationEtablissement',
 'etablissementSiege',
 'complementAdresseEtablissement',
 'numeroVoieEtablissement',
 'indiceRepetitionEtablissement',
 'typeVoieEtablissement',
 'libelleVoieEtablissement',
 'codePostalEtablissement',
 'libelleCommuneEtablissement',
 'libelleCommuneEtrangerEtablissement',
 'distributionSpecialeEtablissement',
 'codeCommuneEtablissement',
 'codeCedexEtablissement',
 'libelleCedexEtablissement',
 'codePaysEtrangerEtablissement',
 'libellePaysEtrangerEtablissement',
 'etatAdministratifEtablissement',
 'count_initial_insee']

reindex_col = ['siren',
 'siret',
 'count_initial_insee',
 'etatAdministratifEtablissement',
 'dateCreationEtablissement',
 'etablissementSiege',
 'complementAdresseEtablissement',
 'numeroVoieEtablissement',
 'indiceRepetitionEtablissement',
 'typeVoieEtablissement',
 'libelleVoieEtablissement',
 'codePostalEtablissement',
 'libelleCommuneEtablissement',
 'libelleCommuneEtrangerEtablissement',
 'distributionSpecialeEtablissement',
 'codeCommuneEtablissement',
 'codeCedexEtablissement',
 'libelleCedexEtablissement',
 'codePaysEtrangerEtablissement',
 'libellePaysEtrangerEtablissement',
 ]


#all_options = {
    #'America': ['New York City', 'San Francisco', 'Cincinnati'],
    #'Canada': [u'Montréal', 'Toronto', 'Ottawa']
#}
app.layout = html.Div([
    html.H1('INSEE'),
    html.P([ html.Br()]),
    dcc.Input(id='insert_siren',
    value='insert SIREN',
    type='text'),
    html.P([ html.Br()]),
    html.Iframe(id = 'datatable', height = 500, width = 1200)
])

@app.callback(
    Output(component_id='datatable', component_property='srcDoc'),
    [Input(component_id='insert_siren', component_property='value'),
    ]
)
def update_output_div(siren):
    conn = sqlite3.connect(r"C:\Users\PERNETTH\Documents\Projects" \
    r"\InseeInpi_matching\Notebooks_matching" \
    r"\programme_matching\App\SQL\insee.db")
    c = conn.cursor()
    query = '''SELECT * FROM INSEE WHERE siren = {} '''.format(siren)
    c.execute(query)
    df = pd.DataFrame(c.fetchall(), columns= insee_col).sort_values(
    by = "etablissementSiege",
     ascending = False)#.reindex( columns = reindex_col)
    return df.to_html()


if __name__ == '__main__':
    app.run_server(debug=True)
