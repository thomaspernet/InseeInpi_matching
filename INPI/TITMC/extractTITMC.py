import re
import xml.etree.ElementTree as ET

def checkbalise(filename):
    """

    """
    to_check = ['{fr:inpi:odrncs:serpentXML}idt',
            '{fr:inpi:odrncs:serpentXML}reps',
            '{fr:inpi:odrncs:serpentXML}etabs',
            '{fr:inpi:odrncs:serpentXML}procs',
            '{fr:inpi:odrncs:serpentXML}acts',
            '{fr:inpi:odrncs:serpentXML}bils']

    tree = ET.parse(filename)
    root = tree.getroot()
    balises = {filename: []}
    mylist= [elem.tag for elem in root.iter()]
    dup_mylist = list(dict.fromkeys(mylist))
    for i in dup_mylist:
        if i in to_check:
            balises[filename].append(i)
    balises[filename] = dup_mylist
    return balises

def getTITMCData(filename):
    """Récupere les données de stocks des fichiers XML des getTITMC

    Les fichiers XML sont complexes, avec un nombre conséquent de children et
    subchildren.

    Les fichiers XML sont composés de plusieurs root. Un root correspond à un
    greffe. De fait, le programme loop sur l'ensemble des root pour récuperer
    les balises.

    Les balises sont repertoriées dans l'élément 'societe', autrement dit, toutes
    les valeurs d'une société sont dans l'élément societe qui est composé
    de plusieurs balises. Une balise contient un vaste ensemble de 'child' et
    'subchild'. Pour récuperer les valeurs des 'child' et 'subchield', il faut
    itérer.

    La balise idt renvoie les données avec des childrens et subchrildren alors
    que les balises 'etabs', 'reps', 'procs', 'acts' ont un premier niveau avant
    d'afficher la donnée.

    Pour récuperer correctement les children et subchildren, on définit un
    dictionnaire contenant 'child' et 'subchild'. 'child' utilise la fonction
    `find` car il n'y a pas de subchild alors que 'subchild' va utiliser
     `findall` puis `iter` pour récuperer l'ensemble des valeurs du sous
     ensemble

    Args:
        filename: String, Nom du fichier avec l'extension `.xml`

    Returns:
        un dictionnaire regroupant toutes les valeurs du fichier xml. Le json
        a la forme suivante:

         {
            'greffe': cod,
            'SIREN': siren,
            'num_gestion': num_gestion,
            'information': {
                        }
    }

    ou
    - greffe -> numéro greffe: obligatoire
    - SIREN -> numéro siren: obligatoire
    - num_gestion -> numéro de gestion du siren: facultatif
    - information: contient les 7 balises:
        - idt :
        - reps
        - etabs
        - procs
        - acts
    un élément n'a pas forcément toutes les balises. De plus, un siren peut
    être utliser plusieurs fois si il y a eu des ajouts. SOuvent via le greffe
    0000
    """
    tree = ET.parse(filename)
    root = tree.getroot()
    data = []
    list_child_its = {
        'child': ['rcs', 'nom_raison_soc',"prenom",'code_form_jur',
        'assoc_unique'],
        'subchild': ['siege',
                 'immat', 'activ', 'radiation','rad', 'commerct',
                 'cap', 'durees', 'divers', 'prem_exer',
                 'dern_ajout', 'domicil', 'eirl', 'auto_ent',
                 'divers'
                 ]
                 }

    list_child_etb = {
        'child': [
        'categ',
        'enseigne',
        'nom_commerc'
             ],
    'subchild': ['activ', 'adr1', 'fonds', 'exploitation',
                  'prec_exploit'
                 ]
            }

    list_child_rep = {
    'child': [
        'qual',
        'rais_nom',
        'nom_denom',
        'prenom',
        'type_dir',
        'dat_crea_rep',
        'dat_modif',
        'dat_radiation'
             ],
    'subchild': ['adr1', 'pers_physique', 'pers_mor'
                 ]
        }

    list_child_procs = {
    'child': [
        'code_obs',
        'texte_obs',
        'dat_obs',
        'num_obs'
             ],
    'subchild': []
    }

    list_child_acts = {
    'child': [
        'type',
        'nature',
        'dat_depot',
        'num_depot_manuel',
        'dat_acte'
             ],
    'subchild': []
    }

    balise = "{fr:inpi:odrncs:serpentXML}"
    for greffe in root.findall("{fr:inpi:odrncs:serpentXML}grf"):
        cod = greffe.get('cod')
        for test_sub in greffe.findall("./{fr:inpi:odrncs:serpentXML}societe"):
            siren = test_sub.get('siren')
            num_gestion = test_sub.get('num_gestion')
            json_data = {
                'greffe': cod,
                'SIREN': siren,
                'num_gestion': num_gestion,
                'information': {
                            }
        }

            # Balise IDT
            for data_ in test_sub.findall(balise + "idt"):
                store_d = {}
                for children in list_child_its['child']:
                    search = balise + children
                    store_d[children] = {}
                    try:  # Parfois n'existe pas, nonetype error
                        item_1 = data_.find(search).text
                    except:
                        item_1 = ''
                    store_d[children] = item_1
                for children in list_child_its['subchild']:
                # pas possible utilser format
                    search = balise + children
                    store_d[children] = {}
                    # Find addresses
                    for child in data_.findall(search):
                        #store_d = {}
                        for l, child_item in enumerate(child.iter()):
                            if l != 0:
                                item_ = child_item.tag
                                item_1 = child_item.text
                                item_ = re.sub(balise, '',
                                           item_)
                                item_1 = re.sub('\n', '', item_1)
                                store_d[children][item_] = item_1
                json_data['information']['idt'] = store_d

            # Balise ETABS*
            store_ = []
            #index_b =[]
            list_subchild = ['etabs', 'reps', 'procs', 'acts']
            for index_, b in enumerate(list_subchild):
                if b == 'etabs':
                    list_to_check = list_child_etb
                    store_d_sub = []
                    #store_d_sub = {}
                elif b == 'reps':
                    list_to_check= list_child_rep
                    store_d_sub = []
                    #store_d_sub_c = {}
                elif b == 'procs':
                    list_to_check=list_child_procs
                    store_d_sub = []
                    #store_d_sub = {}
                else:
                    list_to_check=list_child_acts
                    store_d_sub = []

                for data_ in test_sub.findall(balise + b): # ex: <acts>

                    ### b[:-1] -> enleve le s a la fin du mot pour
                    ### matcher la key
                    b_subchild = b[:-1]

                    for children in data_.findall(balise + b_subchild): # ex :<act>
                    ## ligne dessous peut etre inutile (double check procs)
                        #if b == 'acts' or b == 'reps' or b == 'etabs' or b == 'procs':
                        store_d_sub_ = {b_subchild: {}}
                        #elif b == 'reps':
                        #    store_d_sub_ = {'rep': {}}
                        #elif b == 'etabs':
                    #        store_d_sub_ = {'etab': {}}

                        for children_ in list_to_check['child']: # ex: <type>; <dat_depot>
                            search = balise + children_
                            try:  # Parfois n'existe pas, nonetype error
                                item_1 = children.find(search).text
                            except:
                                item_1 = ''

                            #if b == 'acts' or b == 'reps' or b == 'etabs' or b == 'procs':
                                ### initiate empty subkey in act key
                            store_d_sub_[b_subchild][children_] = {}
                            store_d_sub_[b_subchild][children_] = item_1
                            #elif b == 'reps':
                                ### initiate empty subkey in act key
                            #    store_d_sub_['rep'][children_] = {}
                        #        store_d_sub_['rep'][children_] = item_1
                        #    elif b == 'etabs':
                                ### initiate empty subkey in act key
                        #        store_d_sub_['etab'][children_] = {}
                        #        store_d_sub_['etab'][children_] = item_1
                            #else:
                        #        store_d_sub[children_] = {}
                        #        store_d_sub[children_] = item_1

                        for child_etb in list_to_check['subchild']:
                            # pas possible utilser format
                            search = balise + child_etb
                            if b == 'reps' or b == 'etabs':
                                pass
                            else:
                                store_d_sub[child_etb] = {}
                        # Find addresses
                            for child in children.findall(search):
                            #store_d = {}
                                for l, child_item in enumerate(child.iter()):
                                    if l != 0:

                                        item_ = child_item.tag
                                        item_1 = child_item.text
                                        item_ = re.sub(balise, '',
                                               item_)
                                        item_1 = re.sub('\n', '', item_1)
                                        if b == 'reps' or b == 'etabs':
                                            try:
                                                store_d_sub_[b_subchild][child_etb][item_] = \
                                            item_1
                                            except:
                                                store_d_sub_[b_subchild][child_etb] = \
                                            {}
                                                store_d_sub_[b_subchild][child_etb][item_] = \
                                        item_1

                                        else:
                                            store_d_sub[child_etb][item_] = \
                                            item_1
                        if b == 'acts' or b == 'reps' or b == 'etabs' or b == 'procs':
                            store_d_sub.append(store_d_sub_)

                        #if b == 'reps' and :
                        json_data['information'][b] = store_d_sub








        #####
            data.append(json_data)

    return data
