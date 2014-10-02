#-------------------------------------------------------------------------------
# Name:         Sima_data_auto_import
# Purpose:      Importazione dei dati xml in SIMA in automatico
#
# Author:       luciano crua & rocco pispico
#
# Created:      23/07/2013
# Copyright [2013] Arpa Piemonte, Crua & Pispico
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Parameters:
#   1) id monitoraggio  (21817)
#   2) path della cartella dove copiare i file letti da FTP. Accetta anche formato unc
#           "\\torino\SS02.03\dati\sima\cunicolo_maddalena\dati_auto\"

#parametri
#in rete    :   21817 "\\torino\SS02.03\dati\sima\cunicolo_maddalena\dati_auto\"
#locale     :   21817 "C:\\temp"
#-------------------------------------------------------------------------------

def main():
    #Leggo i parametri della riga di comando
    if len(sys.argv) < 3:
        logger.error('La riga di comando non ha tutti i parametri')
        return
    monit=sys.argv[1]
    nome_dir=sys.argv[2]
    n_scritti = 0
    n_righe=0

    try:
                dbPG = psycopg2.connect("dbname='nina' user='xxxx' host='10.127.144.202' password='xxxxx'")
                datiPG = dbPG.cursor()
                datiPG2 = dbPG.cursor()
                logger.info('Connessione postgresql ok')
    except:
                logger.error("Connessione postgresql errata")
                return

#===========Apro la connessione ftp e leggo i file che ci sono
    ftp_conn = ftplib.FTP ('ftp.xxxx','xxxx','xxxxx')
    ftp_conn.cwd('export_giorn')
    file_misure = ftp_conn.nlst() #faccio un ls della directory e lo memorizzo in file_misure

    dest_mail = list()
    q_mail = 'SELECT auth_user.email, auth_user.first_name FROM sima.auth_user, sima.sima_link_monit_auth_user WHERE auth_user.id = sima_link_monit_auth_user.monuser_auth_id AND sima_link_monit_auth_user.monuser_invio_mail AND sima_link_monit_auth_user.monuser_monit_id =' + monit
    datiPG.execute(q_mail)
    for pp in datiPG:
        dest_mail.append(pp)
    datiPG.close
#===========Leggo ogni file nella directory e lo processo in sql
    for nome in file_misure:
        ftp_conn = ftplib.FTP ('ftp.arpa.piemonte.it','ltfsas','Ltfsas2012')
        ftp_conn.cwd('export_giorn')
        file_xml = leggi_ftp(ftp_conn, nome,nome_dir)
#        file_xml = nome
        if file_xml == 'DIRECTORY VUOTA':
            InviaMail(n_righe,n_scritti,file_xml,dest_mail,'dir_vuota')

        elif file_xml<>'vuoto':
            logger.info('File da importare: '+ file_xml)
            decod_camp.decod_camp(monit, file_xml)
            file_imp = string.replace(os.path.basename(file_xml),'.','_') #uso il nome del file di import per dare il nome al file sql
#inizializzo la Connessione PG
            try:
                dbPG = psycopg2.connect("dbname='nina' user='xxxx' host='10.127.144.202' password='xxxx'")
                datiPG = dbPG.cursor()
                datiPG2 = dbPG.cursor()
                logger.info('Connessione postgresql ok')
            except:
                logger.error("Connessione postgresql errata")
                return
#mi assicuro che l'id monitoraggio esista
            try:
                q1= "SELECT monit_descrizione FROM sima.sima_monitoraggio WHERE monit_id='" + monit + "';"
                datiPG.execute(q1)
                if datiPG.rowcount < 1:
                    logger.error("Monitoraggio con id = " + monit + " inesistente")
                    datiPG.close
                    return
                for pp in datiPG:
                    logger.info('Monitoraggio = ' + str(pp[0]))
                    datiPG.close
            except:
                logger.error("L'id monitoraggio inserito non esiste. qry = " + q1)
                return
#Leggo i nomi dei campi utili ad inserire i valori nel DB
            q1="SELECT campimp_python, campimp_campi_xml FROM sima.sima_campi_tab_import WHERE campimp_python is not null AND campimp_monit_id = " + str(monit)
            datiPG.execute(q1)
            key_sima = dict()
            key_list = datiPG.fetchall()
            datiPG.close
            for rec_nomi in key_list:
                aggiungi={rec_nomi[0]:rec_nomi[1]}
                key_sima.update(aggiungi)
        # apro il file sql da importare in postgres
            nome_sql= 'update_' + file_imp + '.sql'
            file_sql=file(nome_sql,'w')
        # Leggo il file xml e' inserisco ogni record in un dizionario "record" e le chiavi del dizionario in una lista "chiavi"
        # (La stessa struttura l'ho creata leggendo il file csv)
        # utilizzo l'istruzione etree.interparse della libreria lxml per leggere una riga ('row') alla volta
        #per evitare un'overflow della memoria, cosi' posso anche settare l'encoding e non incorrere in errori di lettura
        #
        #questo primo ciclo serve per leggere il nome della tabella, scritto nella prima riga del file.
        #Tale nome diventa una classe di ogni tag, per cui ogni <row> ha davanti il nome della classe (tabella) ad esempio {view_misura_pubb_exp_ext}
        #il nome di tale classe puo' variare da file a file, e viene utilizzato per identificare le <row> dell'xml
#serve per la lettura del file xml con header, senza questa porzione di codice restituisce errore relativo all'URI
            fin = open(file_xml, "r")
            linea = fin.readline(10000)
            find_str=linea[(linea.find('xmlns=')+7):-3]
            fin.close()
            find_tag = '{' + find_str + '}' + 'row'
            idx=0
#---------------------------------------------------------------------------------------------------------------------------------------------------
##            eventi = ("start","start-ns") #questi sono gli eventi, nella lettura dell'XML che vado a cercare per leggere il nome della tabella xmlns
##            tree = etree.iterparse(file_xml, events=eventi) #faccio il parsing, una riga alla volta, solo per gli eventi di prima
##            idx=0
##            for action, elem in tree:
##                print("%s: %s" % (action, elem))
##                find_tag = '{' + elem[1] + '}' + 'row' #elem contiene il nome della classe xlmns (la tabella), costruisco cosi' la stringa del tag da cercare nel parsing successivo
##                print find_tag
##                idx +=1
##                if idx == 2:
##                    break # mi fermo alla seconda lettura, perche' dopo iniziano le <row>
            for _, element in etree.iterparse(file_xml, encoding="ISO-8859-1", tag=find_tag):
                record = {}
                for elemento in element:
                    chiave = elemento.tag.partition('}')[2]
                    #print('%s -:- %s' % (chiave, elemento.text))
                    record[chiave]=elemento.text
                element.clear()

#controllo che nel record xml il tag <valore>  e <data_pubblicazione> sia compilato
#========================================================================================================
##                if (record[key_sima['valore']] == None or record[key_sima['valore']] == '') and (record[key_sima['valore_txt']] == '' or record[key_sima['valore_txt']] == None) :
##                    logger.info("ATTENZIONE!!! Valore testo o numerico VUOTO nel file XML. Id misura LTF = " + record[key_sima['id_parametro_misura']])
###                    continue
##                if (record[key_sima['data_pubblicazione']] == None or record[key_sima['data_pubblicazione']] == ''):
##                    logger.info("ATTENZIONE!!! La data di pubblicazione non e' presente nel file XML. Id misura LTF = " + record[key_sima['id_parametro_misura']])
###                    continue
#==============================================================================================================
#vado a leggere l'ID del parametro
                n_righe= n_righe + 1
                try:
                    txt_qry3="SELECT decparam_parsce_id FROM sima.sima_decod_param_prop WHERE decparam_parsce_id is not null AND decparam_monit_id = " + monit + " AND decparam_codice ='" + record[key_sima['parametro']] + "' ;"
                    datiPG.execute(txt_qry3)
                    if datiPG.rowcount > 0:
                        for pp in datiPG:
                            param_id = pp[0]
#nella tab  grandezza_fisica guardo se il parametro e' di tipo numerico o di tipo testo
                        try:
                            qry_tipo_val="SELECT sima_grandezza_fisica.granfi_tiva_id FROM sima.sima_grandezza_fisica, sima.sima_parametri_scelti WHERE sima_grandezza_fisica.granfi_id = sima_parametri_scelti.parsce_granfi_id AND sima_parametri_scelti.parsce_id = " + str(param_id) + " ;"
                            datiPG.execute(qry_tipo_val)
                            ris_val = datiPG.fetchone()
                            tipo_val=ris_val[0]
 #il parametro e' di tipo testo
                            chk_tipo=True
                            if tipo_val == 6 or tipo_val==9:
                                 chk_tipo=False
                        except:
                            logger.error('Record scritti: ' + str(n_scritti)+' Problemi con la query per il tipo di valore del parametro = ' + qry_tipo_val + ". \n\t Id misura LTF = " + record[key_sima['id_parametro_misura']])
                            return
                    else:
                        logger.error('Record scritti: ' + str(n_scritti)+ "Il parametro " + record[key_sima['parametro']] + " non e' presente nel DB. Id misura LTF = " + record[key_sima['id_parametro_misura']])
                        return
                except:
                    logger.error('Record scritti: ' + str(n_scritti)+' Ci sono problemi con la query del parametro qry = ' + txt_qry3)
                    return
#trovo la matrice e l'ID del punto di misura
                try:
                    txt_qry="SELECT granfi_matr_id FROM sima.sima_grandezza_fisica, sima.sima_parametri_scelti WHERE sima_grandezza_fisica.granfi_id = sima_parametri_scelti.parsce_granfi_id AND parsce_id="+str(param_id)+";"
                    datiPG.execute(txt_qry) #trovo l'ID della matrice
                    if datiPG.rowcount > 0:
                        for pp in datiPG:
                            matrice_id = pp[0]
                        try:
                            txt_qry2="SELECT punmis_id FROM sima.v_sima_decod_punti WHERE decprop_monit_id = " + monit + " AND punmis_matr_id = " + str(matrice_id) + " AND decprop_codif_prop = '" + record[key_sima['stazione']] + "';"
                            #logger.info(txt_qry2)
                            datiPG2.execute(txt_qry2) #trovo l'ID del punto misura
                            if datiPG2.rowcount > 0:
                                for pp in datiPG2:
                                    punto_mis = pp[0]
                            else:
                                logger.error('Record scritti: ' + str(n_scritti)+' La stazione ' + record[key_sima['stazione']] + " non e' presente nel DB. Id misura LTF = " + record[key_sima['id_parametro_misura']])
                        except:
                            logger.error('Record scritti: ' + str(n_scritti)+' Ci sono problemi con il punto di monitotaggio = ' + txt_qry2)
                            return
                    else:
                        logger.error('Record scritti: ' + str(n_scritti)+' La matrice ' + record[key_sima['matrice']] + " non e' presente nel DB. Id misura = " + record[key_sima['id_parametro_misura']])
                except:
                    logger.error('Record scritti: ' + str(n_scritti)+' Ci sono problemi con la matrice = ' + txt_qry)
                    return
#creo il dizionario per associare al periodo di campionamento dei vari parametri che sono campionati con dei periodi prefissati un ora di inzio
                ora_periodo=dict()
                txt_qry_per='SELECT decper_periodo, decper_orainiz, decper_durata FROM sima.sima_decod_periodo WHERE decper_monit_id = '+ str(monit) +' AND decper_matr_id = '+ str(matrice_id) + ";"
                datiPG2.execute(txt_qry_per)
                per_list = datiPG2.fetchall()
                datiPG2.close
                for per_qr in per_list:
                    ora_periodo[per_qr[0]]=datetime.time(time.strptime(per_qr[1], '%H:%M')[3],time.strptime(per_qr[1], '%H:%M')[4])
#vado a leggere l'ID della campagna
                try:
#trovo il n della campagna, potrebbe essere diverso da monitoraggio a monitoraggio
                    n_campag=str(record[key_sima['campagna']].rpartition("_")[2])
                    txt_qry4="SELECT campag_id, campag_datainizio, campag_datafine, campag_extra, campag_descrizione FROM sima.sima_campagna WHERE campag_monit_id = " + monit + " AND campag_matr_id = "  + str(matrice_id) + " AND campag_n = '" + str(n_campag)+ "';"
                    datiPG.execute(txt_qry4)
                    if datiPG.rowcount > 0:
                        for pp in datiPG:
                            campag_id = pp[0]
                            datacamp_ini= pp[1]
                            datacamp_fine= pp[2]
                            descr_camp=pp[4]
                    else:
                        logger.error('Record scritti: ' + str(n_scritti)+' \n La campagna ' + record[key_sima['campagna']] + " non e' presente nel DB. Id misura LTF = " + record[key_sima['id_parametro_misura']])
                except:
                    logger.error('Record scritti: ' + str(n_scritti)+' \n  Id misura LTF = ' + record[key_sima['id_parametro_misura']]+ " Ci sono problemi con la query della campagna qry txt = " + txt_qry4)
                    return
                datiPG2.close
#trovo l'ID del puntoparametro a cui devo associare la misura da inserire nel DB
                try:
                    q1= "SELECT sima_link_punmis_parsce.pumpar_id FROM sima.sima_link_punmis_parsce WHERE sima_link_punmis_parsce.pumpar_parsce_id = '" + str(param_id) + "' "
                    q2= "AND sima_link_punmis_parsce.pumpar_campag_id = '" + str(campag_id) + "' AND sima_link_punmis_parsce.pumpar_punmis_id = '" + str(punto_mis) + "';"
                    datiPG.execute(q1+q2)
                    if datiPG.rowcount < 1:
                        logger.error('Record scritti: ' + str(n_scritti)+' \n Punto misura ' + str(punto_mis) + " inesistente. Id misura LTF = " + record[key_sima['id_parametro_misura']]+'\n\t\t\t\t\t\t'+q1+q2)
                        datiPG.close
                        return
                    for pp in datiPG:
                        puntoparametro = pp[0]
                except:
                    logger.error('Record scritti: ' + str(n_scritti)+' \n Ci sono problemi con la query del puntoparametro (pumpar_id) qry = ' + q1+ q2)
                    return
#in questo blocco verifico che la stessa misura non sia gia' inserita, e che non abbia valori diversi
#==========================================================================================================================================================
                giorno_prel = conv_todate(record[key_sima['data_inizio']],'/')
#se l'ora del prelievo e' nulla gli assegno un valore predefinito (0,0)
                ora_prel=datetime.time(0,0)
                cursore=dbPG.cursor()
#gestisco i valori dov'e' compilato il tag <periodo_campionamento>, questo vale per le matrici VB, Am e Radon. Per il controllo uso una tabella di supporto in cui sono state inserite solo le matrici con il periodo di campionamento assegnato
                cursore.execute("SELECT DISTINCT decper_matr_id FROM sima.sima_decod_periodo WHERE decper_matr_id = " + str(matrice_id)+';')
                if cursore.rowcount >0:
                    for row in cursore.fetchall():
                       ora_prel = ora_periodo[record[key_sima['periodo']]]
#se la query precedente non restituisce nessun record viene controllato che non sia compilato nel xml il tag <ora_campionamento> se si, viene effettuata una conversione in formato data e ora.
                elif record[key_sima['ora_camp']] != '' and record[key_sima['ora_camp']] != None:
                    minuti=0
                    if record[key_sima['ora_camp']].partition(":")[2] != '':
                        minuti=int(record[key_sima['ora_camp']].partition(":")[2])
                    ora_prel= datetime.time(int(record[key_sima['ora_camp']].partition(":")[0]),int(record[key_sima['ora_camp']].partition(":")[2]))
                data_prel=datetime.datetime.combine(giorno_prel,ora_prel)
                data_prel_str = giorno_prel.isoformat() + ' ' + ora_prel.isoformat()
#cerco nel db la misura che devo inserire, per essere sicuro che non sia gia' stata inserita, o che non sia variata la data di validazione. (dato_grezzo e' l'equivalente di misura pubblicata come definita da LTF)
                try:
#uso il COALESCE nella query per prendere il primo valore non nullo tra le variabili passati nella parentesi
                    q1="SELECT misura_valore, misura_valoretesto, COALESCE(to_char(misura_datogrezzo, 'dd/MM/YYYY'),'-') as dg, COALESCE(to_char(misura_datavalidato, 'DD/MM/YYYY'),'--') as dv, misura_id FROM sima.sima_misura where misura_pumpar_id = " + str(puntoparametro) + " AND misura_dataprelievo = '" + data_prel_str + "';"
                    datiPG.execute(q1)
                except:
                    logger.error('Record scritti: ' + str(n_scritti)+' Ci sono problemi con la query per la verifica della misura qry = ' + q1 + " Id misura LTF = " + record[key_sima['id_parametro_misura']])
#la misura esiste, controllo che non sia cambiato il valore e che si debbano solo aggiornare le date
                try:
                    if datiPG.rowcount == 1:
                        misura_db = datiPG.fetchone()
#metto il valore numerico o il valore testo nelle stesse variabili per poter fare una cosa piu' ordinata controllando
                        if chk_tipo:
                            if (record[key_sima['valore']] == None or record[key_sima['valore']] == ''):
                                val_file=None
                            else:
                                val_file=float(record[key_sima['valore']])
                            if (misura_db[0] == None or misura_db[0] == ''):
                                val_db= None
                            else:
                                val_db=float(misura_db[0])
                            txt_err="Valore "
                        else:
                            val_file=str((record[key_sima['valore_txt']]))
                            val_db=str((misura_db[1]))
                            txt_err="Valore testo "
# controllo che il valore misura_valore o misura_valoretesto nel db siano uguali a quelli da XML/CSV se diverse lo segnalo
                        if val_file != val_db:
#===========================ERRORE DA RICERCARE NEL LOG, deve essere gestito in modo manuale================================
                            logger.info(txt_err + "db: " + str(misura_db[0]) + " differente dall' XML " + str(record[key_sima['valore']]) + "; ID_LTF: " + str(record[key_sima['id_parametro_misura']])+ "\n\t\t\t\t\t\t\t SELECT: " +  q1)
                            InviaMail(n_righe,n_scritti,file_xml,dest_mail,'var')
                        else:
#controllo che le date di pubblicazione nel db e nel xml non siano diverse, se lo sono lo segnalo nel log
                            if misura_db[2] != record[key_sima['data_pubblicazione']]:
                                logger.error('Record scritti: ' + str(n_scritti)+' Data pubblicazione differente tra DB e XML. Id misura LTF = ' + record[key_sima['id_parametro_misura']])
#se la data di validazione e' diversa faccio un update aggiornando solo questo valore nel db
                            if misura_db[3] == '--':
##                                q_up = "UPDATE. Id misura LTF = " + record[key_sima['id_parametro_misura']]
##                                logger.info(q_up)
##                                file_sql.write("UPDATE sima.sima_misura set misura_datavalidato = '" + str(record[key_sima['data_validazione']]) + "' WHERE misura_id = " + str(misura_db[4]) + ";\n")
                                q_up = "UPDATE. Id parametro misura LTF = " + record[key_sima['id_parametro_misura']]
                                logger.info(q_up)
                                qry_update = "UPDATE sima.sima_misura set misura_datavalidato = '" + str(record[key_sima['data_validazione']]) + "' WHERE misura_id = " + str(misura_db[4]) + ";"
                                datiPG.execute(qry_update)
                                dbPG.commit()
                            else:
                                if misura_db[3] != record[key_sima['data_validazione']]:
                                    logger.error('Record scritti: ' + str(n_scritti)+' Attenzione!! Data validazione differente tra DB e XML. Id misura LTF = ' + record[key_sima['id_parametro_misura']])
                    elif datiPG.rowcount > 1:
# piu di una misura per gli stessi parametri
                        logger.error('Record scritti: ' + str(n_scritti)+' Piu` di un record inserito nel DB - CONTROLLARE!!! ' + q1)
# costruisco la query per l'INSERT a pezzi, in modo da non mettere i campi che sono NULL
                    else:
                        if record[key_sima['data_pubblicazione']] is not None:
                            data_grz = conv_todate(record[key_sima['data_pubblicazione']],'/')
                            iq2=", misura_datogrezzo"
                            #iq6= ", to_date('" + data_grz.isoformat() + "', 'dd-mm-yyyy')"
# con questa conversione postgres riconosce le date come tali
                            iq6= ", '" + data_grz.isoformat() + "'"
                        else:
                            logger.error('Record scritti: ' + str(n_scritti)+' Manca la data di pubblicazione. ID_LTF: ' + record[key_sima['id_parametro_misura']])
                            iq2=""
                            iq6=""
                        if record[key_sima['data_validazione']] is not None:
                            data_valid = conv_todate(record[key_sima['data_validazione']],'/')
                            iq3=", misura_datavalidato"
                            iq7= ", '" + data_valid.isoformat() + "'"
                            #iq7= ", to_date('" + data_valid.isoformat() + "', 'dd-mm-yyyy')"
                        else:
                            iq3=""
                            iq7=""
                        if record[key_sima['data_fine']] is not None:
                            data_fine = conv_todate(record[key_sima['data_fine']],'/')
                            iq4=", misura_datafineprelievo"
                            iq8= ", '" + data_fine.isoformat() + "'"
                            #iq8= ", to_date('" + data_fine.isoformat() + "', 'dd-mm-yyyy')"
                        else:
                            iq4=""
                            iq8=""
                        if record[key_sima['volume_camp']] is not None:
                            vol_camp=str(record[key_sima['volume_camp']])
                            if vol_camp.replace(',','').isdigit():
                                vol_camp = vol_camp.replace(',','.')
                            iq9=", misura_vol_camp"
                            iq11= ", '" + vol_camp + "'"
                        else:
                            iq9=""
                            iq11=""
                        if record[key_sima['incertezza']] is not None:
                            iq10=", misura_incertezza"
                            iq12= ", '" + str(record[key_sima['incertezza']]) + "'"
                        else:
                            iq10=""
                            iq12=""
                        if record[key_sima['inf_limite']] is not None:
                            inferior='t'
                        else:
                            inferior='f'
#uso la variabile chk_tipo che mi dice se il parametro ha un valore di tipo testo o numerico
                        if chk_tipo:
# insert misura_valore NUMERICO con controllo se il valore e' vuoto, in questo caso inserisce NULL
                            if (record[key_sima['valore']] == None or record[key_sima['valore']] == ''):
                                record[key_sima['valore']] =  'NULL'
                            iq1 = "INSERT INTO sima.sima_misura(misura_pumpar_id, misura_valore, misura_valoreinferioreallimite, misura_dataprelievo"
                            iq5 = ") VALUES (" + str(puntoparametro) + ", " + record[key_sima['valore']] + ", '" + inferior + "', '" + data_prel_str +"'"
                            logger.info ("Inserito valore numerico: " + record[key_sima['valore']] + " ID_LTF: " + record[key_sima['id_parametro_misura']])
                        else:
                            if (record[key_sima['valore_txt']] == '' or record[key_sima['valore_txt']] == None) :
                                 record[key_sima['valore_txt']] = 'NULL'
# insert misura_valore TESTO con controllo se il valore e' vuoto, in questo caso inserisce NULL
                            iq1 = "INSERT INTO sima.sima_misura(misura_pumpar_id, misura_valoretesto, misura_dataprelievo"
                            iq5 = ") VALUES (" + str(puntoparametro) + ", '" + record[key_sima['valore_txt']] + "', '" + data_prel_str +"'"
                            logger.info ("Inserito valore testo: " + record[key_sima['valore_txt']] + " ID_LTF: " + record[key_sima['id_parametro_misura']])
                        if record[key_sima['note']] != None:
                            iq1 = iq1 + ', misura_note'
                            iq5 = iq5 + ", '" + record[key_sima['note']] + "'"
#                        file_sql.write(iq1+iq2+iq3+iq4+iq9+iq10+iq5+iq6+iq7+iq8+iq11+iq12+");\n")
                        qry_insert = iq1+iq2+iq3+iq4+iq9+iq10+iq5+iq6+iq7+iq8+iq11+iq12+");"
                        datiPG.execute(qry_insert)
                        dbPG.commit()
                        n_scritti = n_scritti + 1
                except:
                    logger.error('Record scritti: ' + str(n_scritti)+' \n Ci sono problemi con scrittura derlla query di INSERT; /n/t qry = ' + qry_insert + " Id misura LTF = " + record[key_sima['id_parametro_misura']])
                    return
                datiPG.close
            file_sql.close()
            dbPG.close()
            logger.info('Record letti : ' + str(n_righe)+ ';\t Record scritti: ' + str(n_scritti)+ ';\n\t\t\t\t\t\t\t File importato: '+ file_xml)
            InviaMail(n_righe,n_scritti,file_xml,dest_mail,'import')
            print 'Record scritti: ' + str(n_scritti) + '; File importato: '+ file_xml
        else:
            logger.info ("Il file e'" + file_xml)# segnala quando un file e' vuoto


def InviaMail(n_righe,n_scritti,file_xml,dest_mail,check):
    if check == 'var':
        MyMsg = "le misure dell'XML sono variate da quelle presenti nel DB\nIl File letto e': "+ file_xml# + "ID_LTF: " + str(record[key_sima['id_parametro_misura']]
        msg_subj='VARIAZIONE misure'
    elif check == 'import':
        MyMsg = "le misure importate oggi sono\nRecord letti : "+ str(n_righe)+ ';\nRecord scritti: ' + str(n_scritti)+ ';\nFile importato: '+ file_xml
        msg_subj='Importazione misure'
    else:
        MyMsg = "Non ci sono misure nell' FTP"
        msg_subj='FTP vuoto'
        dest_mail.append('d.vietti@arpa.piemonte.it')

    for destin in dest_mail:
        txt_msg = "Buongiorno "+ destin[1] + ",\n"+ MyMsg
        msg = MIMEText(txt_msg)
        msg['Subject'] = msg_subj
        msg['From'] = "sima_cunicolo@arpap.it"
        msg['To'] = destin[0]
        try:
            connessione = smtplib.SMTP("mail.arpa.piemonte.it")
            connessione.sendmail(destin[0],destin[0], msg.as_string())
            connessione.quit()
        except:
            print "Connessione fallita!"
    return

def leggi_ftp(conn_ftp, file_new,nome_dir):
#Leggo il file "file_new" da FTP lo copio in una directory locale o del server, e lo sposto nelle dir 'elaborati' su FTP
    xml_read = 'vuoto'
    try:
        spazio_dir=conn_ftp.size(file_new)
        if spazio_dir>100 and spazio_dir<500000000:
#faccio la copia del xml sul server ss02.03
#            nome_dir ="\\\\torino\\ss02.03\\dati\\sima\\cunicolo_maddalena\\dati_auto\\"
#            nome_dir = "c:\\tmp\\"
            filexml = open(nome_dir + file_new,'wb') #creo il file su cui fare la copia da FTP
            conn_ftp.retrbinary('RETR '+ file_new, filexml.write) #scrivo sul file locale il file FTP
            filexml.close()
#Apro il file appena creato in locale per la lettura
            filexml2 = open(nome_dir+ file_new,'rb')
            conn_ftp.cwd('elaborati')
            conn_ftp.storbinary("STOR ok_" + file_new, filexml2) #faccio l'upload del filexml2 su FTP nella dir 'elaborati'
            xml_read= nome_dir + file_new # setto il path del file xml di cui fare il parsing
            filexml2.close()
# A COSA SERVEEEEEEE!!!!!!
            conn_ftp.cwd('/prova')
            conn_ftp.delete(file_new) #cancella il file che e' stato spostato precedentemente
        return xml_read
    except:
        logger.error(file_new + " e' una directory.")
        xml_read = 'DIRECTORY VUOTA'
        return xml_read

def conv_todate(dataT,sep):
    if dataT.partition(sep)[0].isdigit():
        data = datetime.date(int(dataT.partition(sep)[2].partition(sep)[2]),int(dataT.partition(sep)[2].partition(sep)[0]),int(dataT.partition(sep)[0]))
    else:
        data = None
    return data

if __name__ == '__main__':
    import datetime
    import time
    import xlrd
    import sys, string, os, logging
    import psycopg2
    import collections
    import smtplib
    from email.mime.text import MIMEText

    import ftplib
    from lxml import etree
    import decod_camp

    outputRis = './'
    LOG_FILENAME = outputRis + "_log.txt"
    logger = logging.getLogger('myapp')
    hdlr = logging.FileHandler(LOG_FILENAME)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)
    logger.removeHandler(hdlr)
    logger.info('Start')
    main()
    logger.info('Stop')
