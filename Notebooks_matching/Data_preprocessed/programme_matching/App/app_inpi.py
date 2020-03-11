import dash, sqlite3, base64, webbrowser
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import numpy as np
from threading import Timer
import os

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
#image_filename = r"C:\Users\PERNETTH\Documents\Projects\InseeInpi_matching" \
#r"\Notebooks_matching\programme_matching\App\calf1.png"

#encoded_image = base64.b64encode(open(image_filename, 'rb').read())

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
inpi_col = [
'Code Greffe', 'Nom_Greffe','Numero_Gestion','RCS_Registre',
'Date_Greffe','Libelle_Evt','ID_Etablissement','siren',
'Nom_Commercial','Enseigne','Date_Début_Activité',
'Domiciliataire_Nom','Domiciliataire_Siren','count_initial_inpi',
'Domiciliataire_Greffe','Domiciliataire_Complément','Type','Siège_PM',
'Activité','Origine_Fonds','Origine_Fonds_Info','Type_Exploitation',
'Pays','Ville','ncc','Code_Postal','Code_Commune',
'Adresse_Ligne1','Adresse_Ligne2','Adresse_Ligne3','Adress_new',
'Adresse_new_clean_reg','possibilite','INSEE','digit_inpi'
,'len_digit_address_inpi',
'Siege_Domicile_Représentant','Activité_Ambulante',
'Activité_Saisonnière','Activité_Non_Sédentaire']

app.layout = html.Div([
    html.H1('INPI'),
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

    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, 'SQL\inpi_origine.db')
    conn = sqlite3.connect(filename)
    #conn = sqlite3.connect(r"C:\Users\PERNETTH\Documents\Projects" \
    #r"\InseeInpi_matching\Notebooks_matching" \
    #r"\programme_matching\App\SQL\inpi_origine.db")

    conn = sqlite3.connect(filename)

    c = conn.cursor()
    query = '''SELECT * FROM INPI WHERE siren = {}
    ORDER BY Ville'''.format(siren)
    c.execute(query)
    df = pd.DataFrame(c.fetchall(), columns= inpi_col)
    return df.to_html()

def open_browser():
	webbrowser.open_new("http://localhost:{}".format(port))

def is_port_in_use(port):
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

port = 8050
port_test = True
while port_test:
    port+=1
    port_test = is_port_in_use(port= port)

if __name__ == '__main__':
    Timer(1, open_browser).start()
    app.run_server(debug=False, port=port)
