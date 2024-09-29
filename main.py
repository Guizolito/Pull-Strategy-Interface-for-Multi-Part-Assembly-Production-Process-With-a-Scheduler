import pandas as pd
import sqlite3
import sys
import math
from PyQt5 import sip
from PyQt5.QtWidgets import QApplication, QMainWindow, QMessageBox, QTableWidget, QTableWidgetItem, QInputDialog, QApplication, QHeaderView
from your_ui_module import Ui_QMainWindow
from PyQt5.QtGui import QColor
import datetime
import psycopg2
from PyQt5.QtCore import QTimer
import os

current_sequence = 1
triggers = 0
emcorte = []
emsequencia = []
class MainWindow(QMainWindow):
    def __init__(self):
        super(MainWindow, self).__init__()
        
        # Load the UI file
        self.ui = Ui_QMainWindow()
        self.ui.setupUi(self)

        # Connect signals and slots for each button
        self.ui.MoveOpCola.clicked.connect(self.onMoveOpColaClicked)
        self.ui.MoveLogCola.clicked.connect(self.onMoveLogColaClicked)
        self.ui.MoveOpCorte.clicked.connect(self.onMoveOpCorteClicked)
        self.ui.MoveAbastBuffer.clicked.connect(self.onMoveAbastBufferClicked)
        self.ui.MoveExp.clicked.connect(self.onMoveExpClicked)
        self.ui.MoveReceitas.clicked.connect(self.onMoveReceitasClicked)
        self.ui.MoveLogCorteNColado.clicked.connect(self.onMoveLogCorteNColadoClicked)
        self.ui.MoveLogCorteColado.clicked.connect(self.onMoveLogCorteColadoClicked)
        
        self.ui.AbastLogCorte.clicked.connect(self.increment_qtd_source)
        #self.ui.AbastLogCorte_2.clicked.connect(self.increment_qtd_source_2)
        self.ui.ConcLogCorte.clicked.connect(self.move_to_buffer)
        self.ui.AbastLogCola.clicked.connect(self.move_to_int)
        self.ui.ConcOpCorte.clicked.connect(self.define_EPs_produced)
        self.ui.ConcOpCorte_2.clicked.connect(self.print_cut_pieces)
        self.ui.ConcOpCola.clicked.connect(self.move_to_concluded)
        self.ui.LimparAvisos.clicked.connect(self.limpar_avisos)
        #self.ui.LimparAvisos_2.clicked.connect(self.limpar_avisos)
        self.ui.ConcLogCorteExp.clicked.connect(self.move_to_expedite)
        self.ui.ExpediteButton.clicked.connect(self.ConcludeExpedition)
        self.ui.ConfirmarNaoColados.clicked.connect(self.UpdateExpedicao)
        self.ui.AdicionarPlanoProducao.clicked.connect(self.show_password_input)
        self.ui.ExcelExpedicao.clicked.connect(self.extract_data_to_excel)
        self.ui.CorteADecorrer.clicked.connect(self.CorteADecorrer)
        self.ui.PesquisaSeq.clicked.connect(self.PesquisaSeq)
        self.ui.EnviarExcedenteOpCola.clicked.connect(self.Enviar_para_Excedente)
        self.ui.AlterarSequencia.clicked.connect(self.AlterarSequencia)
        #self.ui.radioButton.toggled.connect(self.apply_filter)
        #self.ui.radioButton_2.toggled.connect(self.apply_filter)
        #self.ui.radioButton_3.toggled.connect(self.apply_filter)

        #self.ui.radioButton_6.toggled.connect(self.apply_filterOpCorte)
        #self.ui.radioButton_4.toggled.connect(self.apply_filterOpCorte)
        #self.ui.radioButton_5.toggled.connect(self.apply_filterOpCorte)

        # Example table initialization
        self.start_function_sequence()
        self.ui.TableLogTrigger.setColumnCount(1)  # Set the number of columns
        self.ui.TableLogTrigger.setHorizontalHeaderLabels(["Ordem de Produção"])  # Set the column header labels
         
        self.ui.TableLogTrigger.setColumnWidth(0, 700)
        self.populate_table_widget(self.ui.TableLogCola,"TableBuffer",[5])
        
        self.populate_table_widget(self.ui.TableOpCorte,"TableOpCorte",[1,3,4,5,8,9,10,11,12,13,14,19,23,24,25,26,27,28,29,30,31,32,33])
        self.populate_table_widget(self.ui.TableOpCorte_2,"TableOpCorte",[1,3,4,5,8,9,10,11,12,13,14,19,23,24,25,26,27,28,29,30,31,32,33,34,35])
        self.populate_table_widget(self.ui.TableOpCola,"TableOpCola",[3,4,6,7,8,9,10,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,30,31,33])
        self.populate_table_widget(self.ui.TableExp,"TableExp",[3,4,6,7,8,9,10,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,30,33])

        self.populate_table_widget(self.ui.TableLogCorte_C_K,"TableLogCorte_C_K",[1,3,5,7,8,9,10,11,12,14,16,18,19,20,21,22,23,24,25,26,27,28,30,31,33])
        self.populate_table_widget(self.ui.TableReceitas,"TableLogCorte_C_K",[0,3,4,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,24,25,26,27,28,30,31,32,33])
        
        self.populate_table_widget(self.ui.TableLogCorte_C_K_2,"TableOpCorteExecutado",[1,3,4,7,8,9,10,11,12,13,14,15,16,17,18,19,24,25,26,27,28,30,31,32,34,36,35])

        #self.rename_first_column('TableLogCorte','Data')
        #self.print_table_data()

        self.group_table_by_columnsBEFORE("database.postgres","TableBuffer")
        self.group_table_by_columnsTableInt("database.postgres")
        self.update_data_timer = QTimer(self)
        self.update_data_timer.timeout.connect(self.update_table_widgets)
        self.update_data_timer.start(10000)  # Update every 10 seconds (adjust as needed)
        self.update_table_widgets()
        

    def update_table_widgets(self):
        
        self.populate_table_widget(self.ui.TableLogCola,"TableBuffer",[5])

        if self.ui.radioButton.isChecked():
            filter_text = "HE"
        elif self.ui.radioButton_2.isChecked():
            filter_text = "C56"
        elif self.ui.radioButton_3.isChecked():
            filter_text = "K"
        else:
            filter_text = "None"

        self.populate_table_widget(self.ui.TableOpCorte,"TableOpCorte",[1,3,4,5,7,8,9,10,11,12,13,14,19,23,24,25,26,27,28,30,31,32,33],None,filter_text)
        self.populate_table_widget(self.ui.TableReceitas,"TableLogCorte_C_K",[0,3,4,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,24,25,26,27,28,30,31,32,33])

        # Fetch the data from the database table
        if self.ui.radioButton_6.isChecked():
            filter_text = "HE"
        elif self.ui.radioButton_4.isChecked():
            filter_text = "C56"
        elif self.ui.radioButton_5.isChecked():
            filter_text = "K"
        else:
            filter_text = "None"
        
        self.populate_table_widget(self.ui.TableOpCorte_2,"TableOpCorte",[1,3,5,8,9,10,11,12,13,14,19,23,24,25,26,27,28,30,31,32,33,34,35],None, filter_text)

        self.populate_table_widget(self.ui.TableOpCola,"TableOpCola",[3,4,6,7,8,9,10,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,30,31,33])
        self.populate_table_widget(self.ui.TableExp,"TableExp",[3,4,6,7,8,9,10,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,30,33])

        self.populate_table_widget(self.ui.TableLogCorte_C_K,"TableLogCorte_C_K",[1,3,5,7,8,9,10,11,12,14,18,19,20,21,22,23,24,25,26,27,28,30,31,33])
        self.populate_table_widget(self.ui.TableLogCorte_C_K_2,"TableOpCorteExecutado",[1,3,4,7,8,9,10,11,12,13,14,15,16,17,18,19,24,25,26,27,28,30,31,32,34,36,35])
        self.populate_table_widget(self.ui.TableLogExcedentes_3,"excedentes",[0,1,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,23,24,25,26,27,28,29,30,31,32,34,36,35])
        
        conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM scheduled_events')
        rows = cursor.fetchall()
        currenttime = datetime.datetime.now() + datetime.timedelta(hours=3)
        
        # Execute the SQL query to get the row count
        query = 'SELECT COUNT(*) FROM scheduled_events'
        cursor.execute(query)
        
        
        # Fetch the result
        row_count = cursor.fetchone()[0]
        global emcorte
        
        for row in rows:
            
            if currenttime > row[2] and row[4] == 0:
                
                idatual = row[0]
                row_position = self.ui.TableLogTrigger.rowCount()
                self.ui.TableLogTrigger.insertRow(row_position)
                sequenciastring = str(row[1])
                self.ui.TableLogTrigger.setItem(row_position, 0, QTableWidgetItem("Insira as peças não coladas do Conjunto " + str(row[3]) + " da sequência " + str(row[1])))
                cursor.execute('UPDATE tablelogcorte_c_k SET "Colado" = %s WHERE "Colado" IS NULL OR "Colado" = %s AND "seq" = %s',("NColado","",sequenciastring))
                conn.commit()
                cursor.execute('UPDATE scheduled_events SET "aviso" = 1 WHERE "id" = %s',(idatual,))
                conn.commit()
                #cursor.execute('UPDATE tableLogCorte_C_K SET "Colado" = %s WHERE COALESCE("Colado", %s) IN (%s, %s, %s, %s) AND seq = %s', ("NColado", "", "", "-", None, str(row[1])))

        
        for item in range(self.ui.TableLogCorte_C_K.rowCount()):
            QtdNecessaria = float(self.ui.TableLogCorte_C_K.item(item,14).text())
            new_value = float(self.ui.TableLogCorte_C_K.item(item,self.ui.TableLogCorte_C_K.columnCount() - 2).text())
            if QtdNecessaria <= new_value:
                            
                for column in range(self.ui.TableLogCorte_C_K.columnCount()):
                    tableitem = self.ui.TableLogCorte_C_K.item(item,column)
                    tableitem.setBackground(QColor(131, 234, 99))  # Red color
            elif new_value >= 0 and QtdNecessaria > new_value:
                
                for column in range(self.ui.TableLogCorte_C_K.columnCount()):
                    tableitem = self.ui.TableLogCorte_C_K.item(item,column)
                    tableitem.setBackground(QColor(240,230,140))  # Yellow color
            
        for item in range(self.ui.TableLogCorte_C_56.rowCount()):
            QtdNecessaria = float(self.ui.TableLogCorte_C_56.item(item,14).text())
            new_value = float(self.ui.TableLogCorte_C_56.item(item,self.ui.TableLogCorte_C_56.columnCount() - 2).text())
            if QtdNecessaria <= new_value:
                            
                for column in range(self.ui.TableLogCorte_C_56.columnCount()):
                    tableitem = self.ui.TableLogCorte_C_56.item(item,column)
                    tableitem.setBackground(QColor(131, 234, 99))  # Red color
            elif new_value >= 0 and QtdNecessaria > new_value:
                
                for column in range(self.ui.TableLogCorte_C_56.columnCount()):
                    tableitem = self.ui.TableLogCorte_C_56.item(item,column)
                    tableitem.setBackground(QColor(240,230,140))  # Yellow color
            
        
        for item in range(self.ui.TableLogCorte_C_K_2.rowCount()):
            
            abastecido = float(self.ui.TableLogCorte_C_K_2.item(item,self.ui.TableLogCorte_C_K_2.columnCount() - 2).text())
            if abastecido == 0:
                self.ui.TableLogCorte_C_K_2.setRowHidden(item, False)
            else:
                self.ui.TableLogCorte_C_K_2.setRowHidden(item, True)
        
        for item in range(self.ui.TableLogExcedentes_3.rowCount()):
            abastecido = float(self.ui.TableLogExcedentes_3.item(item,self.ui.TableLogExcedentes_3.columnCount() - 2).text())
            if abastecido == 0:
                self.ui.TableLogExcedentes_3.setRowHidden(item, False)
            else:
                self.ui.TableLogExcedentes_3.setRowHidden(item, True)
        
        for item in range(self.ui.TableOpCola.rowCount()):
            
                abastecido = float(self.ui.TableOpCola.item(item,self.ui.TableOpCola.columnCount() - 3).text())
                
                if abastecido == 1:
                
                    self.ui.TableOpCola.setRowHidden(item, False)
                else:
                    self.ui.TableOpCola.setRowHidden(item, True)

        for item in range(self.ui.TableExp.rowCount()):
            
                abastecido = float(self.ui.TableExp.item(item,33).text())
                
                if abastecido == 2:
                
                    for column in range(self.ui.TableExp.columnCount()):
                        tableitem = self.ui.TableExp.item(item,column)
                        tableitem.setBackground(QColor(131, 234, 99))  # Green color
                elif abastecido == 0:
                    for column in range(self.ui.TableExp.columnCount()):
                        tableitem = self.ui.TableExp.item(item,column)
                        tableitem.setBackground(QColor(248, 114, 99))  # red color
                elif abastecido == 1:
                    for column in range(self.ui.TableExp.columnCount()):
                        tableitem = self.ui.TableExp.item(item,column)
                        tableitem.setBackground(QColor(240,230,140))  # yellow color
                elif abastecido == 3:
                    for column in range(self.ui.TableExp.columnCount()):
                        tableitem = self.ui.TableExp.item(item,column)
                        tableitem.setBackground(QColor(50,139,168))  # blue color
                else:
                    self.ui.TableExp.setRowHidden(item, True)

        
        for item in range(self.ui.TableLogCola.rowCount()):
            
                abastecido = float(self.ui.TableLogCola.item(item,self.ui.TableLogCola.columnCount() - 2).text())
                
                if abastecido == 1:
                
                    for column in range(self.ui.TableLogCola.columnCount()):
                        tableitem = self.ui.TableLogCola.item(item,column)
                        tableitem.setBackground(QColor(131, 234, 99))  # Green color
                else:
                    for column in range(self.ui.TableLogCola.columnCount()):
                        tableitem = self.ui.TableLogCola.item(item,column)
                        tableitem.setBackground(QColor(240,230,140))  # yellow color
        
        for item in range(self.ui.TableOpCorte_2.rowCount()):
            
                abastecido = self.ui.TableOpCorte_2.item(item,33).text()
                
                if abastecido in emcorte: 
                
                    for column in range(self.ui.TableOpCorte_2.columnCount()):
                        tableitem = self.ui.TableOpCorte_2.item(item,column)
                        tableitem.setBackground(QColor(131, 234, 99))  # Green color
                else:
                    for column in range(self.ui.TableOpCorte_2.columnCount()):
                        tableitem = self.ui.TableOpCorte_2.item(item,column)
                        tableitem.setBackground(QColor(240,230,140))  # yellow color 

        for item in range(self.ui.TableOpCorte.rowCount()):
            
                abastecido = self.ui.TableOpCorte.item(item,33).text()
                
                if abastecido in emcorte: 
                
                    for column in range(self.ui.TableOpCorte.columnCount()):
                        tableitem = self.ui.TableOpCorte.item(item,column)
                        tableitem.setBackground(QColor(131, 234, 99))  # Green color
                else:
                    for column in range(self.ui.TableOpCorte.columnCount()):
                        tableitem = self.ui.TableOpCorte.item(item,column)
                        tableitem.setBackground(QColor(240,230,140))  # yellow color        

        for item in range(self.ui.TableLogCorte_C_K.rowCount()):
            if self.ui.TableLogCorte_C_K.item(item,23).text() == "Colado" or self.ui.TableLogCorte_C_K.item(item,23).text() == "NColado":
                self.ui.TableLogCorte_C_K.setRowHidden(item, False)
            else:
                self.ui.TableLogCorte_C_K.setRowHidden(item, True)
                #self.ui.TableLogCorte_C_K.setRowHidden(item, False)
    
        for item in range(self.ui.TableLogCorte_C_56.rowCount()):
            if self.ui.TableLogCorte_C_56.item(item,23).text() == "Colado" or self.ui.TableLogCorte_C_56.item(item,23).text() == "NColado":
                self.ui.TableLogCorte_C_56.setRowHidden(item, False)
            else:
                self.ui.TableLogCorte_C_56.setRowHidden(item, True)
                #self.ui.TableLogCorte_C_56.setRowHidden(item, False)
        
        try:
            conn = psycopg2.connect(database="db", host="localhost", user="postgres", password="teste123", port="5432")
            cursor = conn.cursor()

            cursor.execute('SELECT MAX(seq) FROM tableexp')
            row = cursor.fetchone()
            if row and row[0] is not None:
                rows = int(str(row[0]))
            else:
                rows = 0    
            
            if rows == 1:
                seq = 1
                cursor.execute('SELECT exp FROM tableexp WHERE seq = %s', (seq,))
                result = cursor.fetchone()
                if result is not None:
                    check = float(result[0])
                else:
                    check = None
                if  check < 4:
                    cursor.execute('SELECT * FROM tableexp WHERE seq = %s', (seq,))

                    sequencia = cursor.fetchall()
                    NumeroEPs = len(sequencia)
                    #print(NumeroEPs)
                    cursor.execute('SELECT * FROM tablelogcorte_c_k WHERE seq = %s', (seq,))
                    NumeroTotalEPs = len(cursor.fetchall())
                    #print(NumeroTotalEPs)

                    cursor.execute('SELECT "CJEs a Expedir" FROM tableexp WHERE seq = %s', (seq,))
                    Nropossivel = cursor.fetchone()[0]
                    
                    cursor.execute('SELECT "Quantidade CJE" FROM tableexp WHERE seq = %s', (seq,))
                    Nronecessario = cursor.fetchone()[0]

                    cursor.execute('SELECT "secagem" FROM tableexp WHERE seq = %s', (seq,))
                    TempoSecagem = cursor.fetchone()[0]
                    
                    if NumeroEPs < NumeroTotalEPs:
                        cursor.execute('UPDATE tableexp SET exp = 0 WHERE seq = %s', (seq,))
                        
                    elif float(Nropossivel) >= float(Nronecessario) and datetime.datetime.now() > TempoSecagem:
                        
                        cursor.execute('UPDATE tableexp SET exp = 2 WHERE seq = %s', (seq,))
                    elif float(Nropossivel) >= float(Nronecessario) and datetime.datetime.now() < TempoSecagem:
                        
                        cursor.execute('UPDATE tableexp SET exp = 3 WHERE seq = %s', (seq,))    
                    elif float(Nropossivel) <= float(Nronecessario) and datetime.datetime.now() < TempoSecagem:
                        
                        cursor.execute('UPDATE tableexp SET exp = 3 WHERE seq = %s', (seq,)) 
                    else:
                        cursor.execute('UPDATE tableexp SET exp = 1 WHERE seq = %s', (seq,))
                        
                    conn.commit()

                    conn.close()
            elif rows == 0:
                a ="a" 
            else:
                for i in range(1, rows+1):
                    cursor.execute('SELECT * FROM tableexp WHERE seq = %s', (i,))
                    sequencia = cursor.fetchall()
                    NumeroEPs = len(sequencia)
                    #print(NumeroEPs)
                    cursor.execute('SELECT * FROM tablelogcorte_c_k WHERE seq = %s', (i,))
                    NumeroTotalEPs = len(cursor.fetchall())
                    #print(NumeroTotalEPs)
                    cursor.execute('SELECT "CJEs a Expedir" FROM tableexp WHERE seq = %s', (i,))
                    Nropossivel = cursor.fetchone()[0]
                    cursor.execute('SELECT "Quantidade CJE" FROM tableexp WHERE seq = %s', (i,))
                    Nronecessario = cursor.fetchone()[0]
                    cursor.execute('SELECT "secagem" FROM tableexp WHERE seq = %s', (i,))
                    TempoSecagem = cursor.fetchone()[0]
                    cursor.execute('SELECT "exp" FROM tableexp WHERE seq = %s', (i,))
                    Foiexpedido = cursor.fetchone()[0]
                    if float(Foiexpedido) < 4:
                        if NumeroEPs < NumeroTotalEPs:
                            cursor.execute('UPDATE tableexp SET exp = 0 WHERE seq = %s', (i,))
                            
                        elif float(Nropossivel) >= float(Nronecessario) and datetime.datetime.now() > TempoSecagem:
                            
                            cursor.execute('UPDATE tableexp SET exp = 2 WHERE seq = %s', (i,))
                        elif float(Nropossivel) >= float(Nronecessario) and datetime.datetime.now() < TempoSecagem:
                            
                            cursor.execute('UPDATE tableexp SET exp = 3 WHERE seq = %s', (i,))    
                        elif float(Nropossivel) <= float(Nronecessario) and datetime.datetime.now() < TempoSecagem:
                            
                            cursor.execute('UPDATE tableexp SET exp = 3 WHERE seq = %s', (i,)) 
                        else:
                            cursor.execute('UPDATE tableexp SET exp = 1 WHERE seq = %s', (i,))
                    
                    conn.commit()

                conn.close()
                
                
            self.group_table_by_columnsExpedicao("a")
        except (Exception, psycopg2.Error) as error:
            QMessageBox.warning(self,"Erro",f"{error}")
       

    def start_function_sequence(self):
        
        conn = psycopg2.connect(database="db", host="localhost", user="postgres", password="teste123", port="5432")
        cursor = conn.cursor()
        cursor.execute("SELECT atual FROM sequencia")
        currseq = float(cursor.fetchone()[0])
        print("Sequência Atual:" + str(currseq))

        
        
        
        global current_sequence
        #sql_query = """
        #SELECT *
        #FROM "tablelogcorte_c_k"
        #WHERE "QTD Abastecida" > 0
        #ORDER BY "seq" DESC
        #LIMIT 1;
        #"""
        #conn = psycopg2.connect(database="db", host="localhost", user="postgres", password="teste123", port="5432")
        #cursor = conn.cursor()
        #try:
        #    cursor.execute(sql_query)
        #    result = cursor.fetchone()

        #    if result:
                # Access the specific columns in the result
                
        #        current_sequence = int(result[29])
        #        "Sequência atual: " + str(current_sequence)
        #    else:
                
        #        current_sequence = 1
                
        #except psycopg2.Error as e:
        #    print("Error: ", e)

        #finally:
        #    cursor.close()
        #    conn.close()
            
        current_sequence = currseq
        
        

    def adjust_tables(self, table_widget):
        
        header = table_widget.horizontalHeader()       
        if header:
            for column in range(table_widget.columnCount()):
                header.setSectionResizeMode(column, QHeaderView.ResizeToContents)
            for row in range(table_widget.columnCount()):
                header.setSectionResizeMode(column, QHeaderView.ResizeToContents)
        # Get the vertical header of the table widget
        vertical_header = table_widget.verticalHeader()

        # Check if the vertical header exists
        if vertical_header:
            # Iterate over each row and resize it to fit its contents
            for row in range(table_widget.rowCount()):
                table_widget.resizeRowToContents(row)

      
    def populate_table_widget(self,table_widget, table_name, hidden_columns=None, column_order=None,filtertxt = None):
        # Connect to the database
        conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
        cursor = conn.cursor()
        global current_sequence
        
        

        if table_widget == self.ui.TableLogCorte_C_K:
            cursor.execute("SELECT * FROM {} WHERE CAST(Seq AS INTEGER) <= %s".format(table_name),(current_sequence,))
            
            column_names = [description[0] for description in cursor.description]
            if self.ui.PesquisaSeqText_6.toPlainText() != "" and self.ui.PesquisaSeqText_6.toPlainText().isdigit():
                
                sequenciaMostrar = self.ui.PesquisaSeqText_6.toPlainText()
                
                if current_sequence >= int(sequenciaMostrar):
                    
                    cursor.execute("SELECT * FROM {} WHERE Seq = %s".format(table_name),(sequenciaMostrar,))
                    rows = cursor.fetchall()
                    column_names = [description[0] for description in cursor.description]
                    # Print the content of the database
                    cursor.execute('SELECT * FROM {} WHERE Seq = %s AND "Máquina" = %s AND ("Colado" = %s OR "Colado" = %s)'.format(table_name),(sequenciaMostrar,"K","Colado","NColado"))
                    rows = cursor.fetchall()
                    
                
            else:
        
            # Fetch the data from the database table
                cursor.execute("SELECT * FROM {} WHERE CAST(Seq AS INTEGER) <= %s".format(table_name),(current_sequence,))
                rows = cursor.fetchall()
                column_names = [description[0] for description in cursor.description]
                

                # Print the content of the database
                cursor.execute('SELECT * FROM {} WHERE CAST(Seq AS INTEGER) <= %s AND "Máquina" = %s AND ("Colado" = %s OR "Colado" = %s)'.format(table_name),(current_sequence,"K","Colado","NColado"))
                rows = cursor.fetchall()
                
            
            try:
            # Clear the existing contents of the table widget
                table_widget.setRowCount(0)
                table_widget.setColumnCount(0)

            # Set the number of rows and columns in the table widget
            
                table_widget.setRowCount(len(rows))
                table_widget.setColumnCount(len(rows[0]))  # Assuming all rows have the same number of columns
            except:
                a = "a"
            
            # Set the column names as horizontal header labels
            table_widget.setHorizontalHeaderLabels(column_names)

            # Insert the data into the table widget
            try:
                for i, row in enumerate(rows):
                    for j, value in enumerate(row):
                        item = QTableWidgetItem(str(value))
                        table_widget.setItem(i, j, item)
            except:
                a = "a"

            if table_widget == self.ui.TableLogCorte_C_K:
                table_widget2 = self.ui.TableLogCorte_C_56
            
            if self.ui.PesquisaSeqText_6.toPlainText() != "" and self.ui.PesquisaSeqText_6.toPlainText().isdigit():
                
                sequenciaMostrar = self.ui.PesquisaSeqText_6.toPlainText()
                
                if current_sequence >= int(sequenciaMostrar):
                    
                    # Print the content of the database
                    cursor.execute('SELECT * FROM {} WHERE Seq = %s AND ("Máquina" = %s OR "Máquina" = %s) AND ("Colado" = %s OR "Colado" = %s)'.format(table_name),(sequenciaMostrar,"C56","HE","Colado","NColado"))
                    rows = cursor.fetchall()
                    column_names = [description[0] for description in cursor.description]
                    
                
            else:


                # Print the content of the database
                cursor.execute('SELECT * FROM {} WHERE CAST(Seq AS INTEGER) <= %s AND ("Máquina" = %s OR "Máquina" = %s) AND ("Colado" = %s OR "Colado" = %s)'.format(table_name),(current_sequence,"C56","HE","Colado","NColado"))
                
                rows = cursor.fetchall()
                column_names = [description[0] for description in cursor.description]
                
            

            try:
            # Clear the existing contents of the table widget
                table_widget2.setRowCount(0)
                table_widget2.setColumnCount(0)

            # Set the number of rows and columns in the table widget
            
                table_widget2.setRowCount(len(rows))
                table_widget2.setColumnCount(len(rows[0]))  # Assuming all rows have the same number of columns
            except:
                a = "a"
            # Set the column names as horizontal header labels
            table_widget2.setHorizontalHeaderLabels(column_names)

            try:
                # Insert the data into the table widget
                for i, row in enumerate(rows):
                    for j, value in enumerate(row):
                        item = QTableWidgetItem(str(value))
                        table_widget2.setItem(i, j, item)
            except:
                a = "a"
            #Hide specific columns if specified
            if hidden_columns:
                
                for column in hidden_columns:
                        
                        table_widget.setColumnHidden(column, True)
                        table_widget2.setColumnHidden(column, True)


        elif table_widget == self.ui.TableOpCorte:
            
            # Adjust the SQL query based on the selected filter_text
            if filtertxt == "None":
                cursor.execute('SELECT * FROM {} WHERE "QTD Cortada" > 0'.format(table_name))
            elif filtertxt == "HE":
            
                JoinHC = "HC"
                cursor.execute('SELECT * FROM {} WHERE ("Máquina" = %s OR "Máquina" = %s) AND "QTD Cortada" > 0'.format(table_name), (filtertxt,JoinHC))
            
            else:
                
                cursor.execute('SELECT * FROM {} WHERE "Máquina" = %s AND "QTD Cortada" > 0'.format(table_name), (filtertxt,))

            rows = cursor.fetchall()
            
            column_names = [description[0] for description in cursor.description]
            # Clear the existing contents of the table widget
            table_widget.setRowCount(0)
            table_widget.setColumnCount(0)

            # Set the number of rows and columns in the table widget
            table_widget.setRowCount(len(rows))
            table_widget.setColumnCount(len(rows[0])) if rows else table_widget.setColumnCount(0)

            # Set the column names as horizontal header labels
            table_widget.setHorizontalHeaderLabels(column_names)

            # Insert the data into the table widget
            for i, row in enumerate(rows):
                for j, value in enumerate(row):
                    item = QTableWidgetItem(str(value))
                    table_widget.setItem(i, j, item)

            #Hide specific columns if specified
            if hidden_columns:
                for column in hidden_columns:
                    table_widget.setColumnHidden(column, True)

        
        elif table_widget == self.ui.TableReceitas:
            
            # Fetch the data from the database table
            
            cursor.execute("SELECT * FROM {}".format(table_name))
            rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            if self.ui.PesquisaSeqText.toPlainText() != "":
                Sequencia = int(self.ui.PesquisaSeqText.toPlainText())
                # Print the content of the database
                cursor.execute('SELECT * FROM {} WHERE seq = %s ORDER BY seq ASC'.format(table_name),(Sequencia,))
                rows = cursor.fetchall()
                try:
                # Clear the existing contents of the table widget
                    table_widget.setRowCount(0)
                    table_widget.setColumnCount(0)

                # Set the number of rows and columns in the table widget
                
                    table_widget.setRowCount(len(rows))
                    table_widget.setColumnCount(len(rows[0]))  # Assuming all rows have the same number of columns
                except:
                    a = "a"
                
                # Set the column names as horizontal header labels
                table_widget.setHorizontalHeaderLabels(column_names)

                # Insert the data into the table widget
                for i, row in enumerate(rows):
                    for j, value in enumerate(row):
                        item = QTableWidgetItem(str(value))
                        table_widget.setItem(i, j, item)

                #Hide specific columns if specified
                if hidden_columns:
                    
                    for column in hidden_columns:
                            
                            table_widget.setColumnHidden(column, True)

                
            else:
                # Fetch the data from the database table
            
                cursor.execute("SELECT * FROM {}".format(table_name))
                rows = cursor.fetchall()
                column_names = [description[0] for description in cursor.description]
                if self.ui.PesquisaSeqText.toPlainText() == "" or not self.ui.PesquisaSeqText.toPlainText().isdigit():
                    # Print the content of the database
                    cursor.execute('SELECT * FROM {} ORDER BY seq ASC'.format(table_name))
                    rows = cursor.fetchall()
                    try:
                    # Clear the existing contents of the table widget
                        table_widget.setRowCount(0)
                        table_widget.setColumnCount(0)

                    # Set the number of rows and columns in the table widget
                    
                        table_widget.setRowCount(len(rows))
                        table_widget.setColumnCount(len(rows[0]))  # Assuming all rows have the same number of columns
                    except:
                        a = "a"
                    
                    # Set the column names as horizontal header labels
                    table_widget.setHorizontalHeaderLabels(column_names)

                    # Insert the data into the table widget
                    for i, row in enumerate(rows):
                        for j, value in enumerate(row):
                            item = QTableWidgetItem(str(value))
                            table_widget.setItem(i, j, item)

                    #Hide specific columns if specified
                    if hidden_columns:
                        
                        for column in hidden_columns:
                                
                                table_widget.setColumnHidden(column, True)          
            

        elif table_widget == self.ui.TableOpCorte_2:
            
            

            # Adjust the SQL query based on the selected filter_text
            if filtertxt == "None":
                cursor.execute('SELECT * FROM {} WHERE "QTD Cortada" < 1'.format(table_name))
            elif filtertxt == "HE":
                JoinHC = "HC"
                cursor.execute('SELECT * FROM {} WHERE ("Máquina" = %s OR "Máquina" = %s) AND "QTD Cortada" < 1'.format(table_name), (filtertxt,JoinHC))
            
            else:
                
                cursor.execute('SELECT * FROM {} WHERE "Máquina" = %s AND "QTD Cortada" < 1'.format(table_name), (filtertxt,))

            rows = cursor.fetchall()
            
            column_names = [description[0] for description in cursor.description]
            # Clear the existing contents of the table widget
            table_widget.setRowCount(0)
            table_widget.setColumnCount(0)

            # Set the number of rows and columns in the table widget
            table_widget.setRowCount(len(rows))
            table_widget.setColumnCount(len(rows[0])) if rows else table_widget.setColumnCount(0)

            # Set the column names as horizontal header labels
            table_widget.setHorizontalHeaderLabels(column_names)

            # Insert the data into the table widget
            for i, row in enumerate(rows):
                for j, value in enumerate(row):
                    item = QTableWidgetItem(str(value))
                    table_widget.setItem(i, j, item)

            #Hide specific columns if specified
            if hidden_columns:
                for column in hidden_columns:
                    table_widget.setColumnHidden(column, True)

        elif table_widget == self.ui.TableExp:
            
            # Fetch the data from the database table
            
            cursor.execute("SELECT * FROM {}".format(table_name))
            rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            if self.ui.PesquisaSeqText_2.toPlainText() != "" and self.ui.PesquisaSeqText_2.toPlainText().isdigit():
                Sequencia = int(self.ui.PesquisaSeqText_2.toPlainText())
                # Print the content of the database
                cursor.execute('SELECT * FROM {} WHERE seq = %s ORDER BY seq ASC'.format(table_name),(Sequencia,))
                rows = cursor.fetchall()
                try:
                # Clear the existing contents of the table widget
                    table_widget.setRowCount(0)
                    table_widget.setColumnCount(0)

                # Set the number of rows and columns in the table widget
                
                    table_widget.setRowCount(len(rows))
                    table_widget.setColumnCount(len(rows[0]))  # Assuming all rows have the same number of columns
                except:
                    a = "a"
                
                # Set the column names as horizontal header labels
                table_widget.setHorizontalHeaderLabels(column_names)

                # Insert the data into the table widget
                for i, row in enumerate(rows):
                    for j, value in enumerate(row):
                        item = QTableWidgetItem(str(value))
                        table_widget.setItem(i, j, item)

                #Hide specific columns if specified
                if hidden_columns:
                    
                    for column in hidden_columns:
                            
                            table_widget.setColumnHidden(column, True)

                
            else:
                # Fetch the data from the database table
            
                cursor.execute("SELECT * FROM {}".format(table_name))
                rows = cursor.fetchall()
                column_names = [description[0] for description in cursor.description]
                if self.ui.PesquisaSeqText_2.toPlainText() == "" or not self.ui.PesquisaSeqText_2.toPlainText().isdigit():
                    # Print the content of the database
                    cursor.execute('SELECT * FROM {} ORDER BY seq ASC'.format(table_name))
                    rows = cursor.fetchall()
                    try:
                    # Clear the existing contents of the table widget
                        table_widget.setRowCount(0)
                        table_widget.setColumnCount(0)

                    # Set the number of rows and columns in the table widget
                    
                        table_widget.setRowCount(len(rows))
                        table_widget.setColumnCount(len(rows[0]))  # Assuming all rows have the same number of columns
                    except:
                        a = "a"
                    
                    # Set the column names as horizontal header labels
                    table_widget.setHorizontalHeaderLabels(column_names)

                    # Insert the data into the table widget
                    for i, row in enumerate(rows):
                        for j, value in enumerate(row):
                            item = QTableWidgetItem(str(value))
                            table_widget.setItem(i, j, item)

                    #Hide specific columns if specified
                    if hidden_columns:
                        
                        for column in hidden_columns:
                                
                                table_widget.setColumnHidden(column, True)    
            
        elif table_widget == self.ui.TableOpCola:
            if self.ui.PesquisaSeqText_3 != "" and self.ui.PesquisaSeqText_3.toPlainText().isdigit():

                # Fetch the data from the database table
                cursor.execute("SELECT * FROM {} WHERE seq = %s".format(table_name),(self.ui.PesquisaSeqText_3.toPlainText()))
            else:
                # Fetch the data from the database table
                cursor.execute("SELECT * FROM {}".format(table_name))
            rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            # Clear the existing contents of the table widget
            table_widget.setRowCount(0)
            table_widget.setColumnCount(0)

            # Set the number of rows and columns in the table widget
            table_widget.setRowCount(len(rows))
            table_widget.setColumnCount(len(rows[0])) if rows else table_widget.setColumnCount(0)

            # Set the column names as horizontal header labels
            table_widget.setHorizontalHeaderLabels(column_names)

            # Insert the data into the table widget
            for i, row in enumerate(rows):
                for j, value in enumerate(row):
                    item = QTableWidgetItem(str(value))
                    table_widget.setItem(i, j, item)

            #Hide specific columns if specified
            if hidden_columns:
                for column in hidden_columns:
                    table_widget.setColumnHidden(column, True)

        elif table_widget == self.ui.TableLogCola:
            if self.ui.PesquisaSeqText_4.toPlainText() != "" and self.ui.PesquisaSeqText_4.toPlainText().isdigit():

                # Fetch the data from the database table
                cursor.execute("SELECT * FROM {} WHERE seq = %s".format(table_name),(self.ui.PesquisaSeqText_4.toPlainText()))
            else:
                # Fetch the data from the database table
                cursor.execute("SELECT * FROM {}".format(table_name))
            rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            # Clear the existing contents of the table widget
            table_widget.setRowCount(0)
            table_widget.setColumnCount(0)

            # Set the number of rows and columns in the table widget
            table_widget.setRowCount(len(rows))
            table_widget.setColumnCount(len(rows[0])) if rows else table_widget.setColumnCount(0)

            # Set the column names as horizontal header labels
            table_widget.setHorizontalHeaderLabels(column_names)

            # Insert the data into the table widget
            for i, row in enumerate(rows):
                for j, value in enumerate(row):
                    item = QTableWidgetItem(str(value))
                    table_widget.setItem(i, j, item)

            #Hide specific columns if specified
            if hidden_columns:
                for column in hidden_columns:
                    table_widget.setColumnHidden(column, True)

        elif table_widget == self.ui.TableLogCorte_C_K_2:
            if self.ui.PesquisaSeqText_5.toPlainText() != "" and self.ui.PesquisaSeqText_5.toPlainText().isdigit():

                # Fetch the data from the database table
                cursor.execute("SELECT * FROM {} WHERE seq = %s".format(table_name),(self.ui.PesquisaSeqText_5.toPlainText()))
            else:
                # Fetch the data from the database table
                cursor.execute("SELECT * FROM {}".format(table_name))
            rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            # Clear the existing contents of the table widget
            table_widget.setRowCount(0)
            table_widget.setColumnCount(0)

            # Set the number of rows and columns in the table widget
            table_widget.setRowCount(len(rows))
            table_widget.setColumnCount(len(rows[0])) if rows else table_widget.setColumnCount(0)

            # Set the column names as horizontal header labels
            table_widget.setHorizontalHeaderLabels(column_names)

            # Insert the data into the table widget
            for i, row in enumerate(rows):
                for j, value in enumerate(row):
                    item = QTableWidgetItem(str(value))
                    table_widget.setItem(i, j, item)

            #Hide specific columns if specified
            if hidden_columns:
                for column in hidden_columns:
                    table_widget.setColumnHidden(column, True)

        elif table_widget == self.ui.TableLogExcedentes_3:
            if self.ui.PesquisaEPText.toPlainText() != "":
                text = self.ui.PesquisaEPText.toPlainText()
                
                # Fetch the data from the database table
                cursor.execute('SELECT * FROM {} WHERE ep = %s'.format(table_name),(text,))
            else:
                # Fetch the data from the database table
                cursor.execute("SELECT * FROM {}".format(table_name))
            rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            # Clear the existing contents of the table widget
            table_widget.setRowCount(0)
            table_widget.setColumnCount(0)

            # Set the number of rows and columns in the table widget
            table_widget.setRowCount(len(rows))
            table_widget.setColumnCount(len(rows[0])) if rows else table_widget.setColumnCount(0)

            # Set the column names as horizontal header labels
            table_widget.setHorizontalHeaderLabels(column_names)

            # Insert the data into the table widget
            for i, row in enumerate(rows):
                for j, value in enumerate(row):
                    item = QTableWidgetItem(str(value))
                    table_widget.setItem(i, j, item)

            #Hide specific columns if specified
            if hidden_columns:
                for column in hidden_columns:
                    table_widget.setColumnHidden(column, True)

        else:
            # Fetch the data from the database table
            cursor.execute("SELECT * FROM {}".format(table_name))
            rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            # Clear the existing contents of the table widget
            table_widget.setRowCount(0)
            table_widget.setColumnCount(0)

            # Set the number of rows and columns in the table widget
            table_widget.setRowCount(len(rows))
            table_widget.setColumnCount(len(rows[0])) if rows else table_widget.setColumnCount(0)

            # Set the column names as horizontal header labels
            table_widget.setHorizontalHeaderLabels(column_names)

            # Insert the data into the table widget
            for i, row in enumerate(rows):
                for j, value in enumerate(row):
                    item = QTableWidgetItem(str(value))
                    table_widget.setItem(i, j, item)

            #Hide specific columns if specified
            if hidden_columns:
                for column in hidden_columns:
                    table_widget.setColumnHidden(column, True)    
            
        self.adjust_tables(self.ui.TableExp)
        self.adjust_tables(self.ui.TableLogCola)
        self.adjust_tables(self.ui.TableLogCorte_C_K)
        self.adjust_tables(self.ui.TableLogCorte_C_56)
        self.adjust_tables(self.ui.TableLogExcedentes_3)
        self.adjust_tables(self.ui.TableOpCola)
        self.adjust_tables(self.ui.TableOpCorte)
        self.adjust_tables(self.ui.TableOpCorte_2)
        self.adjust_tables(self.ui.TableReceitas)
        self.adjust_tables(self.ui.TableLogCorte_C_K_2)
        # Close the connection
        conn.close()

    

    def insert_merge(self, EPCopiado, database_file, table_widget):
        # Read the static Excel file
        df = pd.read_excel(EPCopiado)

        # Add the new columns to the DataFrame
        df["QTD Abastecida"] = 0
        df["QTD Fornecida"] = 0

        # Connect to the database
        conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
        cursor = conn.cursor()


        # Drop the existing table if it exists
        cursor.execute("DROP TABLE {}".format(table_widget.objectName()))

        # Create the table in the database if it doesn't exist
        create_table_query = "CREATE TABLE IF NOT EXISTS {} (".format(table_widget.objectName())

        # Get the column names from the Excel file
        column_names = df.columns.tolist()

        # Register the column names from the Excel file in the CREATE TABLE query
        for column_name in column_names:
            create_table_query += '"{}" TEXT,'.format(column_name)

        # Add the new columns
        create_table_query += '"QTD_Source" INTEGER,'
        create_table_query += '"QTD_Fornecida" INTEGER'

        create_table_query += ")"

        cursor.execute(create_table_query)

        # Insert the data into the database table
        df.to_sql(table_widget.objectName(), conn, if_exists='replace', index=False)

        # Commit the changes
        conn.commit()

        # Get the column names from the database table
        cursor.execute("PRAGMA table_info({})".format(table_widget.objectName()))
        columns = cursor.fetchall()

        # Print the content of the database
        cursor.execute("SELECT * FROM {}".format(table_widget.objectName()))
        rows = cursor.fetchall()

        # Clear the existing contents of the table widget
        table_widget.setRowCount(0)
        table_widget.setColumnCount(0)

        # Set the number of rows and columns in the table widget
        table_widget.setRowCount(len(rows))
        table_widget.setColumnCount(len(rows[0]))  # Assuming all rows have the same number of columns

        # Insert the data into the table widget
        for i, row in enumerate(rows):
            for j, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                table_widget.setItem(i, j, item)

        # Close the connection
        conn.close()

    def insert_new_merge(self, EPCopiado, database_file, table_widget,sheetname,table_widget2):
        # Read the static Excel file
        df_sheet1 = pd.read_excel(EPCopiado,sheet_name=sheetname)

        # Add the new columns to the DataFrame
        df_sheet1["QTD Abastecida"] = 0
        df_sheet1["QTD Fornecida"] = 0
    
        # Connect to the database
        conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
        cursor = conn.cursor()


        # Drop the existing table if it exists
        cursor.execute("DROP TABLE IF EXISTS TableLogCorte")

        # Create the table in the database if it doesn't exist
        create_table_query = "CREATE TABLE IF NOT EXISTS {} (".format(table_widget.objectName())

        # Get the column names from the Excel file
        column_names = df_sheet1.columns.tolist()

        # Register the column names from the Excel file in the CREATE TABLE query
        for column_name in column_names:
            create_table_query += '"{}" TEXT,'.format(column_name)

        # Add the new columns
        create_table_query += '"QTD_Source" INTEGER,'
        create_table_query += '"QTD_Fornecida" INTEGER'

        create_table_query += ")"

        cursor.execute(create_table_query)

        # Insert the data into the database table
        df_sheet1.to_sql(table_widget.objectName(), conn, if_exists='replace', index=False)

        # Commit the changes
        conn.commit()

        # Get the column names from the database table
        cursor.execute("PRAGMA table_info({})".format(table_widget.objectName()))
        columns = cursor.fetchall()

        # Print the content of the database
        cursor.execute("SELECT * FROM {} WHERE Máquina = %s".format(table_widget.objectName()),("K",))
        rows = cursor.fetchall()

        # Clear the existing contents of the table widget
        table_widget.setRowCount(0)
        table_widget.setColumnCount(0)

        # Set the number of rows and columns in the table widget
        table_widget.setRowCount(len(rows))
        table_widget.setColumnCount(len(rows[0]))  # Assuming all rows have the same number of columns

        # Insert the data into the table widget
        for i, row in enumerate(rows):
            for j, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                table_widget.setItem(i, j, item)


        # Print the content of the database
        cursor.execute("SELECT * FROM {} WHERE Máquina = %s".format(table_widget.objectName()),("C56",))
        rows = cursor.fetchall()

        # Clear the existing contents of the table widget
        table_widget2.setRowCount(0)
        table_widget2.setColumnCount(0)

        # Set the number of rows and columns in the table widget
        table_widget2.setRowCount(len(rows))
        table_widget2.setColumnCount(len(rows[0]))  # Assuming all rows have the same number of columns

        # Insert the data into the table widget
        for i, row in enumerate(rows):
            for j, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                table_widget2.setItem(i, j, item)


        # Close the connection
        conn.close()

    def insert_OpCola(self, EPCopiado, database_file, table_widget): #Creates table OP cola
        # Read the static Excel file
        df = pd.read_excel(EPCopiado,sheet_name="Fluxo Colado")

        # Add the new columns to the DataFrame
        df["QTD Fornecida"] = 0

        # Connect to the database
        conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
        cursor = conn.cursor()


        # Drop the existing table if it exists
        cursor.execute("DROP TABLE {}".format(table_widget.objectName()))

        # Create the table in the database if it doesn't exist
        create_table_query = "CREATE TABLE IF NOT EXISTS {} (".format(table_widget.objectName())

        # Get the column names from the Excel file
        column_names = df.columns.tolist()

        # Register the column names from the Excel file in the CREATE TABLE query
        for column_name in column_names:
            create_table_query += '"{}" TEXT,'.format(column_name)

        # Add the new columns

        create_table_query += '"QTD_Fornecida" INTEGER'

        create_table_query += ")"

        cursor.execute(create_table_query)

        # Insert the data into the database table
        df.to_sql(table_widget.objectName(), conn, if_exists='replace', index=False)

        # Commit the changes
        conn.commit()

        # Get the column names from the database table
        cursor.execute("PRAGMA table_info({})".format(table_widget.objectName()))
        columns = cursor.fetchall()

        # Print the content of the database
        cursor.execute("SELECT * FROM {}".format(table_widget.objectName()))
        rows = cursor.fetchall()

        # Clear the existing contents of the table widget
        table_widget.setRowCount(0)
        table_widget.setColumnCount(0)

        # Set the number of rows and columns in the table widget
        table_widget.setRowCount(len(rows))
        table_widget.setColumnCount(len(rows[0]))  # Assuming all rows have the same number of columns

        # Insert the data into the table widget
        for i, row in enumerate(rows):
            for j, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                table_widget.setItem(i, j, item)

        # Close the connection
        conn.close()

    def create_table_buffer(self):

        # Connect to the database
        #conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
        conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
        cursor = conn.cursor()

        # Drop the existing table if it exists
        #cursor.execute("DROP TABLE TableBuffer")                          # - PARA DAR REFRESH

        # Create the TableBuffer table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS TableBuffer (
                "Data Produção" TEXT, 
                CJE TEXT,
                EP TEXT,              
                QTD INTEGER,
                QTDPossivel INTEGER,
                MINCOLA REAL,
                Seq INTEGER,
                FI INTEGER
            )
        ''')

        # Commit the changes
        conn.commit()

        # Close the connection
        conn.close()

    def create_table_int():

        # Connect to the database
        conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
        cursor = conn.cursor()

        # Create the TableBuffer table if it doesn't exist

        cursor.execute("DROP TABLE IF EXISTS tableint")                          # - PARA DAR REFRESH

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tableint (
                "Data Produção" TEXT, 
                CJE TEXT,
                EP TEXT,              
                QTD INTEGER,
                QTDPossivel INTEGER,
                MINCOLA REAL,
                Seq INTEGER,
                FI INTEGER
            )
        ''')

        # Commit the changes
        conn.commit()

        # Close the connection
        conn.close()

    #create_table_int()

    def insert_mergeTableOpCola(self, EPCopiado, database_file, table_widget):   #Isto é para o OPCORTE
        # Read the static Excel file
        df = pd.read_excel(EPCopiado)

        # Add the new columns to the DataFrame
        df["QTD Abastecida"] = 0
    
        # Connect to the database
        conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
        cursor = conn.cursor()

        # Drop the existing table if it exists
        cursor.execute("DROP TABLE {}".format(table_widget.objectName()))

        # Create the table in the database if it doesn't exist
        create_table_query = "CREATE TABLE IF NOT EXISTS {} (".format(table_widget.objectName())

        # Get the column names from the Excel file
        column_names = df.columns.tolist()

        # Register the column names from the Excel file in the CREATE TABLE query
        for column_name in column_names:
            create_table_query += '"{}" TEXT,'.format(column_name)

        # Add the new columns
        create_table_query += '"QTD_Source" INTEGER'
    

        create_table_query += ")"

        cursor.execute(create_table_query)

        # Insert the data into the database table
        df.to_sql(table_widget.objectName(), conn, if_exists='replace', index=False)

        # Commit the changes
        conn.commit()

        # Close the connection
        conn.close()

    def insert_mergeTableOpCola(self, EPCopiado, database_file, table_widget):   #Isto é para o OPCORTE
        # Read the static Excel file
        df = pd.read_excel(EPCopiado)

        # Add the new columns to the DataFrame
        df["QTD Abastecida"] = 0
    
        # Connect to the database
        conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
        cursor = conn.cursor()

        # Drop the existing table if it exists
        cursor.execute("DROP TABLE {}".format(table_widget.objectName()))

        # Create the table in the database if it doesn't exist
        create_table_query = "CREATE TABLE IF NOT EXISTS {} (".format(table_widget.objectName())

        # Get the column names from the Excel file
        column_names = df.columns.tolist()

        # Register the column names from the Excel file in the CREATE TABLE query
        for column_name in column_names:
            create_table_query += '"{}" TEXT,'.format(column_name)

        # Add the new columns
        create_table_query += '"QTD_Source" INTEGER'
    

        create_table_query += ")"

        cursor.execute(create_table_query)

        # Insert the data into the database table
        df.to_sql(table_widget.objectName(), conn, if_exists='replace', index=False)

        # Commit the changes
        conn.commit()

        # Close the connection
        conn.close()


    def create_replica_table(self, database_name, existing_table_name, new_table_name):
        try:
            # Connect to the SQLite database
            conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
            cursor = conn.cursor()

            # Get the column names and s from the existing table
            cursor.execute("PRAGMA table_info({})".format(existing_table_name))
            columns = cursor.fetchall()

            # Create a list of column names for the new table
            column_names = [col[1] for col in columns]

            # Check if "QTD OK" and "QTD NOK" columns already exist, and add them if not
            if "QTD OK" not in column_names:
                cursor.execute("ALTER TABLE {} ADD COLUMN 'QTD OK' INTEGER".format(new_table_name))

            if "QTD NOK" not in column_names:
                cursor.execute("ALTER TABLE {} ADD COLUMN 'QTD NOK' INTEGER".format(new_table_name))

            # Commit the transaction and close the connection
            conn.commit()
            conn.close()
            
        except sqlite3.Error as e:
            QMessageBox.warning(self,"Erro",f"{e}")

    # Call the function to create the TableBuffer table
    #create_table_buffer()

    def increment_qtd_source(self):
        selected_row_K = self.ui.TableLogCorte_C_K.currentRow()
        selected_row_exp = self.ui.TableLogCorte_C_56.currentRow()
        if self.ui.TableLogCorte_C_K.currentRow() >= 0:
            selected_row = self.ui.TableLogCorte_C_K.currentRow()
            table_widget = self.ui.TableLogCorte_C_K
            table = "tablelogcorte_c_k"
            
        elif self.ui.TableLogCorte_C_56.currentRow() >= 0:
            selected_row = self.ui.TableLogCorte_C_56.currentRow()
            table_widget = self.ui.TableLogCorte_C_56
            table = "tablelogcorte_c_k"

        else:
            selected_row = -1
        if selected_row_K >= 0 and selected_row_exp >= 0:
            QMessageBox.warning(self,"Erro","Selecione apenas uma linha de uma das tabelas")
            selected_row = -1
        if selected_row >= 0:
            qtd_text = 1
            
                
            if qtd_text == 1:
                conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
                cursor = conn.cursor()
                nome_EP = table_widget.item(selected_row, 2).text()
                sequence_number = table_widget.item(selected_row, 29).text()
                qtd_value = int(qtd_text)
                
                
                cursor.execute('SELECT "QTD Abastecida" FROM {} WHERE Seq = %s AND EP = %s'.format(table),(sequence_number,nome_EP))
                current_value = int(cursor.fetchone()[0])
                new_value = int(current_value + qtd_value)
                #item = QTableWidgetItem(str(new_value)
      
                #cursor.execute("UPDATE 'TableLogCorte' SET 'QTD Abastecida' = %s WHERE 'Seq' = %s", (new_value, sequence_number))
                cursor.execute('UPDATE {} SET "QTD Abastecida" = %s WHERE Seq = %s AND EP = %s'.format(table),(str(new_value),sequence_number, nome_EP))
                conn.commit()

                cursor.execute("SELECT * FROM {} WHERE Seq = %s AND EP = %s".format(table),(sequence_number,nome_EP))
                LinhaAdicionada = cursor.fetchall()
                
                # Extract all elements of the first tuple except the last two elements

                # Create a new tuple with the same elements as LinhaAdicionada[0]
                #new_tuple = tuple(LinhaAdicionada[0])

                # Modify the element you want (assuming it's the last element)
                #new_tuple = new_tuple[:-5] + (1,)  # Replace the element at len(new_tuple)-4 with 1

                # Assign the new tuple back to LinhaAdicionada[0]
                #LinhaAdicionada[0] = new_tuple

                # Extract the values you want to insert
                values_to_insert = list(LinhaAdicionada[0][:-2])

                # Convert qtd_text to an integer
                qtd_value = int(qtd_text)

                # Append qtd_value as the last item in values_to_insert
                values_to_insert.append(qtd_value)

                values_to_insert[-3] = 0

                # Create the placeholders for the query
                placeholders = ','.join(['%s' for _ in values_to_insert])

                # Execute the INSERT query with the extracted values
                cursor.execute("INSERT INTO tableopcorte VALUES ({})".format(placeholders), values_to_insert)

                # Commit the changes
                conn.commit()

                
                QtdNecessaria = int(table_widget.item(selected_row, 7).text())
                # Print the corresponding slot in the database
                #print("Updated 'QTD Abastecida' for product '{product_name}' to: {new_value}")

                #self.populate_table_widget(self.ui.TableLogCorte_C_K,"TableLogCorte_C_K",[0,1,4,5,8,9,10,11,12,18,19,20,21,22,27,28])
                

                #self.populate_table_widget(self.ui.TableOpCorte,"TableOpCorte",[0,1,4,5,8,9,10,11,12,14,17,18,19,23,24,25,27,28,29,30,31])
                
                conn.close()
                
                if QtdNecessaria <= new_value:
                    
                    for column in range(table_widget.columnCount()):
                        
                        item = table_widget.item(selected_row, column)
                        item.setBackground(QColor(131, 234, 99))  # Red color
                elif new_value >= 0 and QtdNecessaria > new_value:
                    
                    for column in range(table_widget.columnCount()):
                        item = table_widget.item(selected_row, column)
                        item.setBackground(QColor(240,230,140))  # Yellow color
                
                QMessageBox.warning(self, "Sucesso", "A quantidade de " + str(qtd_value) + " foi inserida na próxima etapa.")
            else:
                # Show alarm message for invalid input
                QMessageBox.warning(self, "Quantidade Inválida", "Por favor, insira uma quantidade válida.")
            
        
        
        else:
            QMessageBox.warning(self, "Linha não selecionada", "Selecione uma linha")

    def increment_qtd_source_2(self):
        
        selected_row = 0
        table_widget = 0
        if selected_row >= 0:
            qtd_text = 1

            if qtd_text == 1:
                qtd_text = 1
                if qtd_text == 1:
                    qtd_value = int(qtd_text)
                    current_value = int(table_widget.item(selected_row, table_widget.columnCount() - 2).text())
                    new_value = current_value + qtd_value
                    CJEatual = table_widget.item(selected_row, 1).text()
                    DATAatual = table_widget.item(selected_row, 0).text()
                    table = 1
                    
                    # Connect to the database
                    conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
                    cursor = conn.cursor()
                    
                    #cursor.execute("UPDATE 'TableLogCorte' SET 'QTD Abastecida' = %s WHERE 'Seq' = %s", (new_value, sequence_number))
                    cursor.execute('UPDATE {} SET "QTD Abastecida" = %s WHERE CJE = %s'.format(table),(str(new_value),CJEatual))
                    conn.commit()
                    #cursor.execute("SELECT 'QTD Abastecida' FROM {table} WHERE CJE = %s",(CJEatual,))
                    

                    #cursor.execute("SELECT * FROM {table} WHERE Seq = %s AND EP = %s",(sequence_number,nome_EP))
                    LinhaAdicionada = []
                    for items in range(table_widget.columnCount()):
                        LinhaAdicionada.append(table_widget.item(selected_row,items).text())
                    integ = 0
                    LinhaAdicionada.append(integ)
                    # Append qtd_value as the last item in values_to_insert
                    LinhaAdicionada.append(qtd_text)

                    # Create the placeholders for the query
                    placeholders = ','.join(['%s' for _ in LinhaAdicionada])
                    
                    # Execute the INSERT query with the extracted values
                    cursor.execute("INSERT INTO TableOpCorte VALUES ({})".format(placeholders), LinhaAdicionada)
                    
                    # Commit the changes
                    conn.commit()
                    

                    # Print the corresponding slot in the database
                    #print("Updated 'QTD Abastecida' for product '{product_name}' to: {new_value}")

                    self.populate_table_widget(self.ui.TableLogCorte_C_K,"TableLogCorte_C_K",[1,3,5,7,8,9,10,11,12,14,18,19,20,21,22,23,24,25,26,27,28,30,31,33])
                    #self.populate_table_widget(self.ui.TableLogCorte_NC_K,"TableLogCorte_NC_K",[0,1,4,5,8,9,10,11,12,18,19,20,21,22,27,28])
                    
                    #self.populate_table_widget(self.ui.TableOpCorte,"TableOpCorte",[0,1,4,5,8,9,10,11,12,14,17,18,19,23,24,25,27,28,29,30,31])
                    
                    conn.close()
                    QtdNecessaria = int(table_widget.item(selected_row, 7).text())
                    new_value = int(table_widget.item(selected_row, table_widget.columnCount() - 2).text())
                    
                    if QtdNecessaria <= new_value:
                        
                        for column in range(table_widget.columnCount()):
                            item = table_widget.item(selected_row, column)
                            item.setBackground(QColor(131, 234, 99))  # Red color
                    elif new_value > 0 and QtdNecessaria > new_value:
                        
                        for column in range(table_widget.columnCount()):
                            item = table_widget.item(selected_row, column)
                            item.setBackground(QColor(240,230,140))  # Yellow color
                    QMessageBox.warning(self, "Sucesso", "A quantidade de " + str(qtd_value) + " foi inserida na próxima etapa.")
                else:
                    # Show alarm message for invalid input
                    QMessageBox.warning(self, "Quantidade Inválida", "Por favor, insira uma quantidade válida.")
            else:
                QMessageBox.warning(self, "Quantidade Inválida", "Insira uma quantidade")
        
        
        else:
            QMessageBox.warning(self, "Linha não selecionada", "Selecione uma linha")

    def move_to_buffer(self):
        selected_row = self.ui.TableLogCorte_C_K_2.currentRow()
        
        if self.ui.TableLogCorte_C_K_2.currentRow() >= 0 and self.ui.TableLogExcedentes_3.currentRow() <= 0:
            selected_row = self.ui.TableLogCorte_C_K_2.currentRow()
            table_widget = self.ui.TableLogCorte_C_K_2
            table = "tableopcorteexecutado"
        
            
        elif self.ui.TableLogExcedentes_3.currentRow() >= 0 and self.ui.TableLogCorte_C_K_2.currentRow() <= 0:
            selected_row = self.ui.TableLogExcedentes_3.currentRow()
            table_widget = self.ui.TableLogExcedentes_3
            table = "excedentes"
        else:
            selected_row = -1
        if selected_row >= 0:
            conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")  # Replace with your database file
                
            cursor = conn.cursor()
            if table == "tableopcorteexecutado":
            
                idatual = self.ui.TableLogCorte_C_K_2.item(selected_row, 36).text()
                
                if idatual:
                    cursor.execute('SELECT abastecido FROM tableopcorteexecutado WHERE "id" = %s',(idatual,))
                    buscar = str(cursor.fetchone()[0])
                    
                else:
                    buscar = "1"
                buscar2 = "1"
            else:

                idatual2 = self.ui.TableLogExcedentes_3.item(selected_row,36).text()
                
                if idatual2:
                    cursor.execute('SELECT abastecido FROM excedentes WHERE "id" = %s',(idatual2,))
                    buscar2 = str(cursor.fetchone()[0])
                    
                else:
                    buscar2 = "1"
                buscar = "1"
            
            

            
            if buscar != "1" or buscar2 != "1":
                
                colado = table_widget.item(selected_row,23).text()
                if colado == "Colado":
                    # Get the first 4 values from the selected row in "TableLogCorte"
                    values = []
                    
                    for column in range(3):
                        item = table_widget.item(selected_row, column)
                        values.append(item.text())
                    
                    # Get the value of "QTD Fornecida"
                    #qtd_fornecida = int(table_widget.item(selected_row,29).text())
                    conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")  # Replace with your database file
                    cursor = conn.cursor()
                    if table == "tableopcorteexecutado":
                        # Connect to the database
                        
                        
                        qtd_atual = float(table_widget.item(selected_row,33).text()) #Quantidade Adicionada

                        #qtd_nova = qtd_fornecida + qtd_atual
                        seqatual = table_widget.item(selected_row,29).text()
                        epatual = table_widget.item(selected_row,2).text()
                        # Append "QTD Fornecida" to the values list
                        values.append(qtd_atual)
                        

                        values.append(int(table_widget.item(selected_row,33).text())/int(table_widget.item(selected_row,31).text()))
                        values.append(float(table_widget.item(selected_row,9).text())*float(table_widget.item(selected_row,33).text())/float(table_widget.item(selected_row,3).text()))
                        values.append(table_widget.item(selected_row,29).text())
                        values.append(table_widget.item(selected_row,31).text())



                        soma = float(table_widget.item(selected_row,9).text())*float(table_widget.item(selected_row,33).text())/float(table_widget.item(selected_row,3).text())
                        

                        values.append(table_widget.item(selected_row,5).text())
                        
                        
                        
                        # Connect to the database
                        conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")  # Replace with your database file
                        cursor = conn.cursor()

                        # Insert the values into "TableBuffer"
                        cursor.execute('INSERT INTO TableBuffer ("Data Produção", CJE, EP, "QTD Cortada", QTDPossivel, MINCOLA, SEQ, FI, Artigo) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s)', values)
                        conn.commit()
                        colado = "Colado"
                        cursor.execute('SELECT "Quantidade EP",fi FROM tablelogcorte_c_k WHERE seq = %s AND "Colado" = %s AND EP = %s',(seqatual,colado,epatual))
                        epadicionado = cursor.fetchall()
                        numerador = float(epadicionado[0][0])/float(epadicionado[0][1])
                        
                        QuantidadeCJEs = float(table_widget.item(selected_row,11).text())
                        
                        tempototal = float(table_widget.item(selected_row,9).text())
                        
                        cursor.execute('SELECT DISTINCT EP,"Quantidade EP",fi FROM tablelogcorte_c_k WHERE seq = %s AND "Colado" = %s',(seqatual,colado))
                        totallinhas = cursor.fetchall()
                        denominador = 0
                        
                        for row in totallinhas:
                            denominador += float(row[1])/float(row[2])
                        
                        Tempoadicionado = (tempototal * numerador / denominador)/(float(epadicionado[0][1])*QuantidadeCJEs)*qtd_atual
                        
                        id = table_widget.item(selected_row, 36).text()
                    
                       
                        
                        cursor.execute('UPDATE {} SET abastecido = 1 WHERE id = %s'.format(table),(id,))
                        conn.commit()
                        conn.close()

                        
                    

                        
                        self.group_table_by_columnsBEFORE("database.postgres","TableBuffer",Tempoadicionado)
                        QMessageBox.warning(self, "Sucesso", "A quantidade de " + str(qtd_atual) + " foi inserida no buffer")
                    else:
                        qtd_atual = float(table_widget.item(selected_row,33).text())
                        QtdAssociadaText = self.ui.QtdAssociadaText.toPlainText()
                        SeqAssociadaText = self.ui.SeqAssociadaText.toPlainText()
                        if QtdAssociadaText != "" and QtdAssociadaText.isdigit() and float(QtdAssociadaText) > 0 and float(QtdAssociadaText) <= qtd_atual and SeqAssociadaText != "" and SeqAssociadaText.isdigit() and float(SeqAssociadaText) > 0:
                            qtd_colocada = int(self.ui.QtdAssociadaText.toPlainText())
                            

                            #qtd_nova = qtd_fornecida + qtd_atual
                            seqatual = self.ui.SeqAssociadaText.toPlainText()
                            epatual = table_widget.item(selected_row,2).text()
                            # Append "QTD Fornecida" to the values list
                            values.append(qtd_colocada)
                            

                            values.append(qtd_colocada/int(table_widget.item(selected_row,31).text()))
                            values.append(float(table_widget.item(selected_row,9).text())*qtd_colocada/float(table_widget.item(selected_row,3).text()))

                            values.append(seqatual)
                            values.append(table_widget.item(selected_row,31).text())
                            soma = float(table_widget.item(selected_row,9).text())*qtd_colocada/float(table_widget.item(selected_row,3).text())
                            cursor.execute('SELECT artigo FROM tablelogcorte_c_k WHERE seq = %s',(seqatual))
                            artigo = str(cursor.fetchone()[0])
                            values.append(artigo)

                            colado = "Colado"
                            cursor.execute('SELECT "Quantidade EP",fi FROM tablelogcorte_c_k WHERE seq = %s AND "Colado" = %s AND EP = %s',(seqatual,colado,epatual))
                            epadicionado = cursor.fetchall()
                            numerador = float(epadicionado[0][0])/float(epadicionado[0][1])
                            print(numerador)
                            cursor.execute('SELECT "Quantidade CJE","Tempo Colagem / min" FROM tablelogcorte_c_k WHERE seq = %s AND EP = %s',(seqatual,epatual))
                            quantidade = cursor.fetchall()
                            QuantidadeCJEs = float(quantidade[0][0])
                            print(QuantidadeCJEs)
                            tempototal = float(quantidade[0][1])
                            print(tempototal)
                            cursor.execute('SELECT DISTINCT EP,"Quantidade EP",fi FROM tablelogcorte_c_k WHERE seq = %s AND "Colado" = %s',(seqatual,colado))
                            totallinhas = cursor.fetchall()
                            denominador = 0
                            
                            for row in totallinhas:
                                denominador += float(row[1])/float(row[2])
                            
                            Tempoadicionado = (tempototal * numerador / denominador)/(float(epadicionado[0][1])*QuantidadeCJEs)*qtd_colocada
                            
                            
                            

                            # Insert the values into "TableBuffer"
                            cursor.execute('INSERT INTO TableBuffer ("Data Produção", CJE, EP, "QTD Cortada", QTDPossivel, MINCOLA, SEQ, FI, Artigo) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s)', values)
                            conn.commit()
                            quantidadefinal = qtd_atual-qtd_colocada
                            id = table_widget.item(selected_row, 36).text()
                            cursor.execute('UPDATE {} SET "QTD OK" = %s WHERE id = %s'.format(table),(quantidadefinal,id,))
                            conn.commit()
                            cursor.execute('UPDATE {} SET abastecido = 1 WHERE "QTD OK" = 0'.format(table))
                            conn.commit()
                            self.group_table_by_columnsBEFORE("database.postgres","TableBuffer",soma)
                            QMessageBox.warning(self, "Sucesso", "A quantidade de " + str(qtd_colocada) + " foi inserida no buffer")
                        else:
                            QMessageBox.warning(self, "Quantidades ou Sequência Incorretas", "Por favor insira valores corretos.")
                    

                    
                    
                else:
                    QMessageBox.warning(self, "EP selecionado é não colado", "O EP selecionado destina-se à expedição. Por favor verifique a linha selecionada.")
            else:
                QMessageBox.warning(self, "Linha Indisponível", "A linha que tentou enviar ou não existe ou foi já enviada. Por favor espere que a aplicação atualize.")
        else:
            QMessageBox.warning(self, "Linha não selecionada", "Selecione apenas uma linha")
    
    def move_to_concluded(self):
        selected_row = self.ui.TableOpCola.currentRow()
        if selected_row >= 0:

            conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")  # Replace with your database file
            cursor = conn.cursor()
            
            
            
            query = """
                SELECT MIN(sum_qtdpossivel)
                FROM (
                    SELECT SUM(qtdpossivel::numeric) as sum_qtdpossivel
                    FROM tableint 
                    GROUP BY "Data Produção", SEQ, CJE, EP
                ) subquery
            """
            cursor.execute(query)

            # Fetch the result
            min_qtdpossivel = math.floor(float(cursor.fetchone()[0]))

            sequencia = self.ui.TableOpCola.item(selected_row, 29).text()
            cursor.execute('SELECT * FROM tablelogcorte_c_k WHERE seq = %s AND "Colado" = %s',(sequencia,"Colado"))
            arows = cursor.fetchall()
            epsnecessarios = []
            for item in arows:
                
                epsnecessarios.append(item[2])

            NumeroPeçasColadas = len(arows)
            #print("necessário: " + str(NumeroPeçasColadas))
            cursor.execute('SELECT DISTINCT ep FROM tableopcola WHERE seq = %s AND exp = 1',(sequencia,))
            brows = cursor.fetchall()
            epsatuais = []
            for item in brows:
                
                epsatuais.append(item[0])
            NumeroPeçasCompletas = len(brows)

            if NumeroPeçasColadas <= NumeroPeçasCompletas:
                if self.ui.QtdOpCola.toPlainText() != "" and min_qtdpossivel >= float(self.ui.QtdOpCola.toPlainText()):
                    # Get the first 4 values from the selected row in "TableLogCorte"
                    
                    
                    seq = self.ui.TableOpCola.item(selected_row,29).text()
                    

                    destination_table = "tableexp"
                    #for row in range(self.ui.TableOpCola.rowCount()):
                    #    if  self.ui.TableOpCola.item(row,29).text() == seq:
                    #        self.ui.TableOpCola.setRowHidden(row, True)

                    # Connect to the database
                    conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")  # Replace with your database file
                    cursor = conn.cursor()
                
                    ep = self.ui.TableOpCola.item(selected_row,2).text()
                    
                    cursor.execute('SELECT * FROM tableexp WHERE seq = %s AND "Colado" = %s',(seq,"Colado"))
                    todas = cursor.fetchall()
                    if len(todas) == 0:
                        try:
                            data_query = 'SELECT * FROM TableOpCola WHERE seq = %s AND "Colado" = %s'
                            cursor.execute(data_query,(seq,"Colado"))
                            rows_to_insert = cursor.fetchall()
                            if rows_to_insert:
                                rows_to_insert_processed = []
                                
                                for row in rows_to_insert:
                                    row_processed = ["".join(filter(str.isdigit, row[0]))] + list(row[1:-1])
                                    
                                    rows_to_insert_processed.append(row_processed)


                                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'tableexp'")
                                
                                column_names = ['"{}"'.format(row[0]) for row in cursor.fetchall() if row[0] != 'secagem']  # Exclude the 'id' column
                                

                        
                                column_list = ','.join(column_names)
                                placeholders = ','.join(['%s' for _ in column_names])


                                for row in rows_to_insert_processed:
                                    cursor.execute("INSERT INTO tableexp VALUES ({})".format(placeholders), row)
                                    
                                
                                
                                conn.commit()

                            
                            insert_data_query = 'UPDATE tableexp SET "QTD Fornecida" = CAST(fi AS INTEGER) * %s WHERE seq = %s AND "Colado" = %s'.format(destination_table)
                            cursor.execute(insert_data_query,(float(self.ui.QtdOpCola.toPlainText()),seq,"Colado"))
                            conn.commit()
                        except (Exception, psycopg2.Error) as error:
                            QMessageBox.warning(self,"Erro",f"{error}")

                    else:
                        sql_query = """
                            SELECT table_name, column_name, data_type
                            FROM information_schema.columns
                            WHERE table_name = 'tableexp'
                            AND table_schema = 'public' -- or specify your schema
                            ORDER BY table_name, ordinal_position;
                        """

                        # Execute the SQL query
                        cursor.execute(sql_query)

                        # Fetch all rows from the result set
                        rows = cursor.fetchall()

                        # Print out the column information
                        


                        for row in todas:
                            novoep = str(row[2])
                            cursor.execute('SELECT fi FROM tableopcola WHERE seq = %s AND ep = %s',(seq,novoep))
                            QuantidadedeEPs = float(cursor.fetchone()[0])*float(self.ui.QtdOpCola.toPlainText())
                            cursor.execute('UPDATE TableExp SET "QTD Fornecida" = "QTD Fornecida" + %s WHERE seq = %s AND ep = %s',(QuantidadedeEPs,seq,novoep))
                            conn.commit()
                    cursor.execute('SELECT * FROM tableexp WHERE seq = %s AND ep = %s',(seq,ep))
                    todas = cursor.fetchall()
                        
                    
                    
                    
                    self.group_table_by_columnsExpedicao("database.postgres")
                    
                    


                    #Até aqui colocamos tudo em expedição, falta subtrair à quantidade atual ou remover
                    quantidadeEnviado = float(self.ui.QtdOpCola.toPlainText())
                    

                    cursor.execute('SELECT DISTINCT EP FROM tableint WHERE seq = %s',(seq,))
                    rows = cursor.fetchall()

                    for row in rows:
                        
                        EPAtual = str(row[0])
                    
                        cursor.execute('SELECT seq, ep, qtdpossivel, id FROM tableint WHERE seq = %s AND ep = %s', (seq, EPAtual))
                        rows = cursor.fetchall()

                        subtracaoemfalta = quantidadeEnviado
                        
                        for row in rows:
                            quantity = int(row[2])
                            
                            subtraction = min(subtracaoemfalta,quantity)
                            
                            new_quantity = quantity - subtraction

                            cursor.execute('UPDATE tableint SET qtdpossivel = %s WHERE id = %s', (new_quantity,row[3]))
                            conn.commit()
                            subtracaoemfalta -= subtraction
                            
                            if subtracaoemfalta <= 0:
                                break
                        cursor.execute('UPDATE tableint SET qtd = fi * qtdpossivel')

                
                        conn.commit()

                    cursor.execute('SELECT * FROM tableint WHERE seq = %s',(seq,))
                    rows = cursor.fetchall()

                    for row in rows:
                        idatual = row[9]
                        EPAtual = str(row[2])
                        qtdatual = row[4]
                        seq = row[6]
                        if qtdatual == 0:
                            cursor.execute('DELETE FROM tableint WHERE id = %s',(idatual,))
                            conn.commit()
                            

                    self.group_table_by_columnsTableInt("database.postgres")
                    QMessageBox.warning(self,"Conclusão de CJE", "A quantidade de " + self.ui.QtdOpCola.toPlainText() + " foi concluída")
                    # Connect to the database
                    
                    #self.populate_table_widget(self.ui.TableExp,"TableExp",[0,4,6,7,8,9,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,30])
                    cursor.execute('SELECT secagem FROM tableexp WHERE seq = %s',(seq,))
                    
                    tempoatual = cursor.fetchone()[0]
                    
                    if str(tempoatual) == '2021-01-01 00:00:00':
                        cursor.execute('SELECT cje FROM tableopcola WHERE seq = %s',(seq,))
                        CJEatual = str(cursor.fetchone())
                        cursor.execute('SELECT "CJE" FROM conjuntos24h')
                        CJEstodos = cursor.fetchall()
                        cje_exists = False

                        # Iterate through the result set to check if desired_cje exists
                        for row in CJEstodos:
                            if CJEatual in row:
                                cje_exists = True
                                break


                        if cje_exists:
                            scheduled = datetime.datetime.now() + datetime.timedelta(hours=24)
                        else:
                            scheduled = datetime.datetime.now() + datetime.timedelta(hours=8)
                        try:
                                cursor.execute("UPDATE tableexp SET secagem = %s WHERE seq = %s",(scheduled,seq))
                                conn.commit()
                                
                        except sqlite3.Error as e:
                            QMessageBox.warning(self,"Erro",f"{e}")

                        formatted_datetime = (
                                (scheduled)
                                .strftime("%Y-%m-%d %H:%M:%S")
                            )
                        
                        QMessageBox.warning(self, "Sucesso.", "Conjunto a terminar às " + formatted_datetime)

                else:
                    QMessageBox.warning(self, "Quantidade inválida ou superior ao total possível", "Insira uma quantidade")
            else:
                QMessageBox.warning(self, "Partes em falta","Partes em falta para concluir assembly. Por favor verifique com Logístico B")
            
        else:
            QMessageBox.warning(self, "Linha não selecionada", "Selecione uma linha")

    def move_to_expedite(self):
        conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")  # Replace with your database file
        cursor = conn.cursor()
        selected_row_buf = self.ui.TableLogCorte_C_K_2.currentRow()
        selected_row_exp = self.ui.TableLogExcedentes_3.currentRow()
        if self.ui.TableLogCorte_C_K_2.currentRow() >= 0:
            selected_row = self.ui.TableLogCorte_C_K_2.currentRow()
            table_widget = self.ui.TableLogCorte_C_K_2
            table = "tableopcorteexecutado"
            
        elif self.ui.TableLogExcedentes_3.currentRow() >= 0:
            selected_row = self.ui.TableLogExcedentes_3.currentRow()
            table_widget = self.ui.TableLogExcedentes_3
            
            table = "excedentes"
        else:
            selected_row = -1
        if selected_row_buf >= 0 and selected_row_exp >= 0:
            
            selected_row = -1
        if selected_row >= 0:
            
            seq = table_widget.item(selected_row,29).text()
            destination_table = "tableexp"

            if table == "tableopcorteexecutado":
            
                idatual = self.ui.TableLogCorte_C_K_2.item(selected_row, 36).text()
                
                if idatual:
                    cursor.execute('SELECT abastecido FROM tableopcorteexecutado WHERE "id" = %s',(idatual,))
                    buscar = str(cursor.fetchone()[0])
                    
                else:
                    buscar = "1"
                buscar2 = "1"
            else:

                idatual2 = self.ui.TableLogExcedentes_3.item(selected_row,36).text()
                
                if idatual2:
                    cursor.execute('SELECT abastecido FROM excedentes WHERE "id" = %s',(idatual2,))
                    buscar2 = str(cursor.fetchone()[0])
                    
                else:
                    buscar2 = "1"
                buscar = "1"
            
            

            
            if buscar != "1" or buscar2 != "1":
            
                conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")  # Replace with your database file
                cursor = conn.cursor()
            
                ep = table_widget.item(selected_row,2).text()
                colado = table_widget.item(selected_row,23).text()

                if colado != "Colado" or table == "excedentes":
                    selected_id = table_widget.item(selected_row,table_widget.columnCount() - 1).text()
                    cursor.execute('SELECT * FROM tableexp WHERE seq = %s AND ep = %s',(seq,ep))
                    todas = cursor.fetchall()
                    if table == "tableopcorteexecutado":
                        if len(todas) == 0:
                            new_row = []
                            for col in range(table_widget.columnCount()-3):
                                item = table_widget.item(selected_row, col)
                                if col != 32:
                                    if item is not None:
                                        new_item = item.text()
                                        new_row.append(new_item)
                                    else:
                                        new_row.append("")  # Create an empty item for missing data

                            placeholders = ','.join(['%s' for _ in new_row])

                            cursor.execute("INSERT INTO tableexp VALUES ({})".format(placeholders), new_row)
                            conn.commit()              
                        else:
                            QuantidadeFornecida = int(table_widget.item(selected_row,33).text())
                            
                            cursor.execute('UPDATE tableexp SET "QTD Fornecida" = "QTD Fornecida" + %s WHERE seq = %s AND ep = %s',(QuantidadeFornecida,seq,ep))
                        
                        
                        cursor.execute('UPDATE {} SET abastecido = 1 WHERE id = %s'.format(table),(selected_id,))
                        conn.commit()
                        QMessageBox.warning(self,"Expedição com Sucesso", "A quantidade de " + table_widget.item(selected_row,33).text() + " foi expedita")
                        self.group_table_by_columnsExpedicao("database.postgres") #Atualiza tableexp
                        # Connect to the database
                    else:
                        QuantidadeFornecida = int(table_widget.item(selected_row,33).text())
                        QtdAssociadaText = self.ui.QtdAssociadaText.toPlainText()
                        SeqAssociadaText = self.ui.SeqAssociadaText.toPlainText()
                        if QtdAssociadaText != "" and QtdAssociadaText.isdigit() and float(QtdAssociadaText) > 0 and float(QtdAssociadaText) <= QuantidadeFornecida and SeqAssociadaText != "" and SeqAssociadaText.isdigit() and float(SeqAssociadaText) > 0:
                            QtdEnviada = int(QtdAssociadaText)
                            seqatual = self.ui.SeqAssociadaText.toPlainText()
                            cursor.execute('SELECT * FROM tableexp WHERE seq = %s AND ep = %s',(seqatual,ep))
                            todas = cursor.fetchall()
                            if len(todas) == 0:
                                new_row = []
                                for col in range(table_widget.columnCount()-3):
                                    item = table_widget.item(selected_row, col)
                                    if col != 32:
                                        if col == 33:
                                            new_item = QTableWidgetItem(str(QtdEnviada))
                                            new_row.append(new_item)
                                        elif col == 29:
                                            new_item = QTableWidgetItem(str(seqatual))
                                            new_row.append(new_item)
                                        else:
                                            if item is not None:
                                                new_item = item.text()
                                                new_row.append(new_item)
                                            else:
                                                new_row.append("")  # Create an empty item for missing data

                                placeholders = ','.join(['%s' for _ in new_row])

                                cursor.execute("INSERT INTO {} VALUES ({})".format(destination_table,placeholders), new_row)
                                conn.commit()              
                            else:
                                                
                                cursor.execute('UPDATE tableexp SET "QTD Fornecida" = "QTD Fornecida" + %s WHERE seq = %s AND ep = %s',(QtdEnviada,seq,ep))

                            id = table_widget.item(selected_row, 36).text()
                            cursor.execute('UPDATE excedentes SET "QTD OK" = "QTD OK" - %s WHERE id = %s',(QtdEnviada,id))
                            conn.commit()
                            cursor.execute('UPDATE excedentes SET abastecido = 1 WHERE "QTD OK" = 0')
                            conn.commit()
                            cursor.execute('UPDATE excedentes SET abastecido = 0 WHERE "QTD OK" > 0')
                            conn.commit()
                            
                            
                            QMessageBox.warning(self,"Expedição com Sucesso", "A quantidade de " + str(QtdEnviada) + " foi expedita")
                            self.group_table_by_columnsExpedicao("database.postgres") #Atualiza tableexp
                            # Connect to the database
                        else:
                            QMessageBox.warning(self, "Quantidade Incorreta", "Por favor insira quantidades válidas de excedente a expedir.")
                
                else:
                    QMessageBox.warning(self, "EP selecionado é Colado", "O EP selecionado deverá destinar-se à colagem. Verifique a linha selecionada.")
            else:
                QMessageBox.warning(self, "Linha Indisponível", "A linha que tentou enviar ou não existe ou foi já enviada. Por favor espere que a aplicação atualize.")
        else:
            QMessageBox.warning(self, "Linha não selecionada", "Selecione uma linha")

    def check_lote(self, lote):
        ret = 0  # Default value
        
        # Ensure lote is at least 10 characters long to avoid index out of range error
        if len(lote) == 6 and lote.isdigit():
        
            ret = 1
        
        return ret

    def define_EPs_produced(self):
        selected_row = self.ui.TableOpCorte.currentRow()
        conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")  # Replace with your database file
        cursor = conn.cursor()
        if selected_row >= 0:
            
            
            if self.ui.QtdOpCorte.toPlainText() != "" and self.ui.QtdOpCorte_2.toPlainText() != "" and self.ui.QtdOpCorte_Total.toPlainText():

                # Get the value of "QTD Fornecida"
                qtd_fornecida = int(self.ui.QtdOpCorte.toPlainText())
                

                # Connect to the database
                

                #cursor = conn.cursor()
                table_widget = self.ui.TableOpCorte
                id_selecionado = int(table_widget.item(selected_row,table_widget.columnCount()-3).text())
                id_string = str(table_widget.item(selected_row,table_widget.columnCount()-3).text())
                cursor.execute("SELECT id FROM tableopcorte WHERE id = %s", (id_string,))
                result = cursor.fetchone()
                if result is not None:
                    check = result[0]
                else:
                    check = None

                if check and check != "0":
                    # Create a new row by copying the selected row's items
                    new_row = []
                    for col in range(table_widget.columnCount() - 1):
                        item = table_widget.item(selected_row, col)
                        if item is not None:
                            new_item = item.text()
                            new_row.append(new_item)
                        elif col == table_widget.columnCount()-3:
                            new_row.append("")  # Create an empty item for missing data
                        else:
                            new_row.append("")  # Create an empty item for missing data

                    # Calculate "QTD OK" and "QTD NOK"
                    
                    
                    
                    if  self.ui.QtdOpCorte.toPlainText().isdigit() and self.ui.QtdOpCorte_2.toPlainText().isdigit() and self.ui.QtdOpCorte_Total.toPlainText().isdigit():
                        qtd_ok_value = int(self.ui.QtdOpCorte.toPlainText())
                        qtd_necessaria_value = int(self.ui.QtdOpCorte_2.toPlainText())
                        qtd_totalok_value = int(self.ui.QtdOpCorte_Total.toPlainText())
                        
                        qtd_excedentes = qtd_ok_value - qtd_necessaria_value  
                        if qtd_ok_value > 0 and qtd_necessaria_value >= 0 and qtd_totalok_value >= 0:
                            if qtd_ok_value <= qtd_totalok_value and qtd_necessaria_value <= qtd_ok_value:
                                qtd_nok_value = qtd_totalok_value - qtd_ok_value
                                if qtd_excedentes <= 0:
                                    qtd_excedentes = 0
                                
                                else:   # Inserir Excedentes como linha na tabela (não é necessário registo, ele apenas busca os valores na tabela)
                                    
                                    epatual = table_widget.item(selected_row, 2).text()
                                    cursor.execute('SELECT * FROM excedentes WHERE EP = %s',(epatual,))
                                    todas = cursor.fetchall()
                                    if len(todas) == 0:
                                        
                                        new_row_exc = []
                                        for col in new_row:
                                            new_row_exc.append(col)

                                        new_row_exc[table_widget.columnCount() -3] = str(qtd_excedentes)
                                        new_row_exc[table_widget.columnCount()-2] = str(0)
                                        row_position = self.ui.TableLogExcedentes_3.rowCount()
                                        
                                        placeholders = ','.join(['%s' for _ in new_row_exc])

                                        cursor.execute('INSERT INTO excedentes VALUES ({})'.format(placeholders), new_row_exc)
                                        conn.commit()
                                        #for j, value in enumerate(new_row_exc):
                                        #    item = QTableWidgetItem(str(value))
                                        #    self.ui.TableLogExcedentes_3.setItem(row_position, j, item)
                                    else:
                                        cursor.execute('SELECT "QTD OK" FROM excedentes WHERE EP = %s',(epatual,))
                                        quantidadeAtual = int(cursor.fetchone()[0])
                                        quantidadefinal = quantidadeAtual + qtd_excedentes
                                        cursor.execute('UPDATE excedentes SET "QTD OK" = %s WHERE EP = %s',(quantidadefinal,epatual))
                                        cursor.execute('UPDATE excedentes SET abastecido = 0 WHERE "QTD OK" > 0')
                                        QMessageBox.warning(self, "Sucesso", "A quantidade de " + str(qtd_excedentes) + " foi inserida como excedente")
                                        conn.commit()


                                
                                if qtd_necessaria_value > 0:
                                    
                                    new_row[table_widget.columnCount() -3] = str(qtd_necessaria_value)

                                    new_row[table_widget.columnCount()-2] = str(qtd_nok_value)
                                    

                                    # Create the placeholders for the query
                                    placeholders = ','.join(['%s' for _ in new_row])

                                    # Execute the INSERT query with the extracted values
                                    cursor.execute("INSERT INTO TableOpCorteExecutado VALUES ({})".format(placeholders), new_row)
                                    

                                    # Commit the changes
                                    conn.commit()
                                    QMessageBox.warning(self, "Sucesso", "A quantidade de " + str(qtd_necessaria_value) + " foi inserida no buffer")
                                cursor.execute("DELETE FROM tableopcorte WHERE id = %s",(id_selecionado,))
                                conn.commit()
                                
                                
                            else:
                                QMessageBox.warning(self, "Quantidade Incorreta", "A quantidades OK é superior ao número previsto de EPs ou a quantidade necessária é maior que a quantidade OK. Por favor verifique os valores")
                        else:
                            QMessageBox.warning(self, "Quantidades Negativas", "Quantidades negativas. Verificar valores.")
                    else:
                        QMessageBox.warning(self, "Quantidade Incorreta", "Quantidades não válidas, possívelmente por serem valores negativos. Verificar valores.")
                else:
                    QMessageBox.warning(self, "Linha Indisponível", "A linha que tentou enviar ou não existe ou foi já enviada. Por favor espere que a aplicação atualize.")
            else:
                QMessageBox.warning(self, "Quantidade em falta", "Insira as quantidades em falta")
        else:
            QMessageBox.warning(self, "Linha não selecionada", "Selecione uma linha")

    def move_to_int(self):
        conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
        cursor = conn.cursor()
        selected_row = self.ui.TableLogCola.currentRow()
        if selected_row >= 0:
            idatual = self.ui.TableLogCola.item(selected_row, 10).text()
            cursor.execute('SELECT * FROM tablebuffer WHERE "id" = %s',(idatual,))
            idatual = cursor.fetchone()
            if idatual is not None:
                sequencia = self.ui.TableLogCola.item(selected_row, 6).text()
                cursor.execute('SELECT * FROM tablelogcorte_c_k WHERE seq = %s AND "Colado" = %s',(sequencia,"Colado"))
                arows = cursor.fetchall()
                epsnecessarios = []
                for item in arows:
                    
                    epsnecessarios.append(item[2])

                NumeroPeçasColadas = len(arows)
                #print("necessário: " + str(NumeroPeçasColadas))
                cursor.execute('SELECT DISTINCT ep FROM tablebuffer WHERE seq = %s',(sequencia,))
                brows = cursor.fetchall()
                epsatuais = []
                for item in brows:
                    
                    epsatuais.append(item[0])
                NumeroPeçasCompletas = len(brows)
                #print("atual: " + str(NumeroPeçasCompletas))

                DataProd = self.ui.TableLogCola.item(selected_row,0).text()
                SEQ = self.ui.TableLogCola.item(selected_row,6).text()
                CJE = self.ui.TableLogCola.item(selected_row,1).text()
                EP = self.ui.TableLogCola.item(selected_row,2).text()
                idretirado = self.ui.TableLogCola.item(selected_row,10).text()
                
                values = []
            
                cursor.execute('SELECT * FROM TableBuffer WHERE "Data Produção" = %s AND SEQ = %s AND CJE = %s AND EP = %s AND id = %s',(DataProd,SEQ,CJE,EP, idretirado))
                LinhaSelecionada = cursor.fetchall()
                values = list(LinhaSelecionada[0][:-2])
                # Create the placeholders for the query
                placeholders = ','.join(['%s' for _ in values])

                # Execute the INSERT query with the extracted values
                cursor.execute("INSERT INTO tableint VALUES ({})".format(placeholders), values)

                # Commit the changes
                conn.commit()
            
            
                cursor.execute('DELETE FROM TableBuffer WHERE "Data Produção" = %s AND SEQ = %s AND CJE = %s AND EP = %s AND id = %s',(DataProd,SEQ,CJE,EP, idretirado))
                conn.commit()


                self.group_table_by_columnsTableInt("database.postgres")
                self.group_table_by_columns("database.postgres","TableBuffer",str(SEQ))
                cursor.close()
                conn.close()
                QMessageBox.warning(self, "Sucesso", "A quantidade de " + str(values[3]) + " foi inserida para colagem")

                if NumeroPeçasColadas > NumeroPeçasCompletas:
                    
                    epsfalta = [ep for ep in epsnecessarios if ep in epsatuais]
                    missing_items_str = ", ".join(map(str, epsfalta))
                    
            else:
                QMessageBox.warning(self, "Linha Indisponível", "A linha que tentou enviar ou não existe ou foi já enviada. Por favor espere que a aplicação atualize.")
        else:
            QMessageBox.warning(self, "Linha não selecionada", "Selecione uma linha")

    def group_table_by_columns(self,database_file, table_name, sequenciadalinha): #WITH 8h INTERVAL
        # Connect to the database
        conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
        cursor = conn.cursor()

        TimeSum = 0

        query = '''
            WITH ranked_rows AS (
                SELECT
                    SEQ,
                    EP,
                    SUM(qtdpossivel::numeric) AS TotalQuantity,
                    SUM(mincola::numeric) AS TotalTime,
                    RANK() OVER (PARTITION BY SEQ ORDER BY SUM(qtdpossivel::numeric)) AS rank
                FROM
                    TableBuffer
                GROUP BY
                    SEQ,
                    EP
            )
            SELECT
                SEQ,
                EP,
                TotalQuantity,
                TotalTime
            FROM
                ranked_rows
            WHERE
                rank = 1;
        '''
        
        cursor.execute(query)
        rows = cursor.fetchall()  # Fetch all rows with the smallest TotalQuantity for each Seq
        #conn.commit()
        for row in rows:
            
            sequencia = str(row[0])
            
            cursor.execute('SELECT * FROM tablelogcorte_c_k WHERE seq = %s AND "Colado" = %s',(sequencia,"Colado"))
            arows = cursor.fetchall()

            NumeroPeçasCompletas = len(arows)
            #print("Peças Totais: " + str(NumeroPeçasCompletas))
            cursor.execute('SELECT DISTINCT ep FROM tablebuffer WHERE seq = %s',(sequencia,))
            brows = cursor.fetchall()
            
            NumeroPeçasColadas = len(brows)
            #print("Peças Atuais Para Colar: " + str(NumeroPeçasColadas))
            if NumeroPeçasColadas >= NumeroPeçasCompletas:
                
                TimeSum += float(row[3])/60 #(Min Cola do menor)
            

        
        
        self.ui.label_10.setText(str(round(TimeSum,2)))
        
        

        if TimeSum < 2:
            
            
            conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
            cursor = conn.cursor()
            # Execute the SQL query to retrieve values of the column "seq"
            cursor.execute("SELECT DISTINCT seq FROM scheduled_events")

            # Fetch all rows from the result set
            rows = cursor.fetchall()

            # Extract values of the column "seq" into a list
            seq_values = [row[0] for row in rows]
            
            # Check if the given value (sequencia) is in the list of seq_values
            if int(sequenciadalinha) not in seq_values:    
                

                #self.ui.TableLogTrigger_2.insertRow(rowcount)
                #self.ui.TableLogTrigger_2.setItem(rowcount, 0, add2)
                cursor.execute('SELECT * FROM TableLogCorte_C_K WHERE Seq = %s AND "Colado" = %s',(sequenciadalinha,"Colado"))
                
                
                rows = cursor.fetchall()
                cursor.execute('SELECT * FROM TableLogCorte_C_K WHERE Seq = %s',(sequenciadalinha,))
                rowsnaocolado = cursor.fetchall()
                
                if len(rows) > 0 and len(rowsnaocolado)-len(rows) > 0:
                    
                    TempoColagem = float(rows[0][9])
                    CJEatual = rows[0][1]
                    
                    scheduled = datetime.datetime.now() + datetime.timedelta(minutes=TempoColagem)
                    cursor.execute('SELECT cje FROM TableLogCorte_C_K WHERE seq = %s',(sequenciadalinha,))
                    CJEatual = str(cursor.fetchone())
                    cursor.execute('SELECT "CJE" FROM conjuntos24h')
                    CJEstodos = cursor.fetchall()
                    cje_exists = False

                    # Iterate through the result set to check if desired_cje exists
                    for row in CJEstodos:
                        if CJEatual in row:
                            cje_exists = True
                            break


                    if cje_exists:
                        scheduled = scheduled + datetime.timedelta(hours=24)
                    else:
                        scheduled = scheduled + datetime.timedelta(hours=8)

                    try:
                        cursor.execute("INSERT INTO scheduled_events (seq, scheduled_timestamp, cje) VALUES (%s, %s, %s)",(sequenciadalinha, scheduled, CJEatual ))
                        conn.commit()
                        #print("Scheduled event inserted successfully.")
                    except sqlite3.Error as e:
                        QMessageBox.warning(self,"Erro",f"{e}") 
                        
                    formatted_datetime = (
                            (scheduled)
                            .strftime("%Y-%m-%d %H:%M:%S")
                        )
                    
                    QMessageBox.warning(self, "Sucesso", "Ordem de Produção de não colados a ser enviada às " + formatted_datetime)

        # Close the connection
        conn.close()

    

    def group_table_by_columnsBEFORE(self,database_file,tablename,soma=0): #Para passar à próxima sequência apenas
        # Connect to the database
        conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
        cursor = conn.cursor()

        TimeSum = 0

        query = '''
            WITH ranked_rows AS (
                SELECT
                    SEQ,
                    EP,
                    SUM(qtdpossivel::numeric) AS TotalQuantity,
                    SUM(mincola::numeric) AS TotalTime,
                    ROW_NUMBER() OVER (PARTITION BY SEQ ORDER BY SUM(qtdpossivel::numeric)) AS row_num
                FROM
                    TableBuffer
                GROUP BY
                    SEQ,
                    EP
            )
            SELECT
                SEQ,
                EP,
                TotalQuantity,
                TotalTime
            FROM
                ranked_rows
            WHERE
                row_num = 1;
        '''

        cursor.execute(query)
        rows = cursor.fetchall()  # Fetch all rows with the smallest TotalQuantity for each Seq
        
        #conn.commit()
        for row in rows:
            
            sequencia = str(row[0])
            
            cursor.execute('SELECT * FROM tablelogcorte_c_k WHERE seq = %s AND "Colado" = %s',(sequencia,"Colado"))
            arows = cursor.fetchall()

            NumeroPeçasCompletas = len(arows)

            cursor.execute('SELECT DISTINCT ep FROM tablebuffer WHERE seq = %s',(sequencia,))
            brows = cursor.fetchall()
            
            NumeroPeçasColadas = len(brows)
            
            if NumeroPeçasColadas >= NumeroPeçasCompletas:
                cursor.execute('UPDATE tablebuffer SET estado = 1 WHERE seq = %s',(sequencia,))
                conn.commit()
               
                TimeSum += float(row[3])/60 #(Min Cola do menor)
            else:
                cursor.execute('UPDATE tablebuffer SET estado = 0 WHERE seq = %s',(sequencia,))
                conn.commit()
                

        

        


        #print("tempo total de buffer: " + str(TimeSum))
        self.ui.label_10.setText(str(round(TimeSum,2)))
        
        temposoma = float(soma)
        
        if temposoma > 0:
            #temposoma = temposoma * 60
            print("Tempo a Somar:" + str(temposoma))
            cursor.execute('UPDATE contadorbuffer SET tempobuffer = tempobuffer + %s',(temposoma,))
            conn.commit()

        
        global current_sequence
        cursor.execute("SELECT atual FROM sequencia")
        currseq = float(cursor.fetchone()[0])

        cursor.execute('SELECT SUM(DISTINCT "Tempo Colagem / min") AS total_distinct_time FROM tablelogcorte_c_k WHERE seq <= %s',(currseq,))
        tempo = cursor.fetchone()
        
        if tempo is not None and tempo[0] is not None:
            tempototal = float(tempo[0])
            
        else:
            tempototal = 0
            
        cursor.execute('SELECT tempobuffer FROM contadorbuffer')
        resultado = cursor.fetchone()
        if resultado is not None:

            temposub = resultado[0]
        else:
            temposub = 0
            
        print("Diferença = " + str(tempototal-temposub))
        if tempototal - temposub < 120:
            curseq = current_sequence
            cursor.execute('SELECT seq FROM tablelogcorte_c_k WHERE seq = %s',(curseq,))
            numerorows = cursor.fetchall()
            if len(numerorows) > 1:
                cursor.execute('UPDATE sequencia SET atual = atual + 1')
                curseq = current_sequence + 1
            conn.commit()
        
        # Close the connection
        conn.close()
        

    def limpar_avisos(self):
        selected_row = self.ui.TableLogTrigger.currentRow()
        #selected_row_NC = self.ui.TableLogTrigger_2.currentRow()
        if selected_row >= 0:
            self.ui.TableLogTrigger.removeRow(selected_row)
        else:
            QMessageBox.warning(self, "Linha não selecionada", "Selecione uma linha")



    def group_table_by_columnsTableInt(self,database_file): #Updates tableOpCola
        try:
            # Connect to the database
            conn = psycopg2.connect(database="db", host="localhost", user="postgres", password="teste123", port="5432")
            cursor = conn.cursor()

            # Execute the query
            query = 'SELECT "Data Produção", SEQ, CJE, EP, SUM(QTD::numeric) AS TotalQuantity, SUM(qtdpossivel::numeric) as QtdPossivel FROM tableint GROUP BY "Data Produção", SEQ, CJE, EP'
            
            cursor.execute(query)
            rows = cursor.fetchall()
            query = 'SELECT DISTINCT seq FROM tableint'
            cursor.execute(query)
            sequencias = cursor.fetchall()
            for i in range(len(sequencias)):
                query = 'SELECT "Data Produção", SEQ, CJE, EP, SUM(QTD::numeric) AS TotalQuantity, SUM(qtdpossivel::numeric) as QtdPossivel FROM tableint WHERE seq = %s GROUP BY "Data Produção", SEQ, CJE, EP'    
                
                sequenciaatual = sequencias[i]
                
                cursor.execute(query,sequenciaatual)
                rows = cursor.fetchall()

                cursor.execute('SELECT ep FROM tableopcola WHERE seq = %s AND "Colado" = %s',(sequenciaatual,"Colado"))
                arows = cursor.fetchall()
                
                NumeroPeçasColadas = len(arows)
                
                #print("necessário: " + str(NumeroPeçasColadas))
                cursor.execute('SELECT DISTINCT ep FROM tableint WHERE seq = %s',(sequenciaatual,))
                brows = cursor.fetchall()
                
                NumeroPeçasCompletas = len(brows)
                
                #print("atual: " + str(NumeroPeçasCompletas))
                if NumeroPeçasColadas > NumeroPeçasCompletas:
                    
                    min_qtdpossivel = 0
                   
                    for row in arows:
                        if row not in brows:
                        
                            cursor.execute('UPDATE tableopcola SET exp = 0 WHERE seq = %s AND ep = %s',(sequenciaatual,str(row[0])))
                            conn.commit()
                else:
                    
                    query = """
                        SELECT MIN(sum_qtdpossivel)
                        FROM (
                            SELECT SUM(qtdpossivel::numeric) as sum_qtdpossivel
                            FROM tableint 
                            WHERE seq = %s
                            GROUP BY "Data Produção", SEQ, CJE, EP
                        ) subquery
                    """
                    cursor.execute(query,(sequenciaatual,))

                    # Fetch the result
                    min_qtdpossivel = str(cursor.fetchone()[0])

                    conn.commit()

                # Execute the query
                

                for row in rows:
                    DataProd = row[0]
                    SEQ = str(row[1])
                    CJE = str(row[2])
                    EP = str(row[3])
                    Qtd = str(row[4])
                    
                    

                    cursor.execute('UPDATE TableOpCola SET "QTD Fornecida" = %s WHERE Seq = %s AND EP = %s', (Qtd, SEQ, EP))
                    conn.commit()
                    cursor.execute('UPDATE TableOpCola SET "ConjPossiveis" = %s WHERE Seq = %s AND EP = %s', (min_qtdpossivel, SEQ, EP))
                    conn.commit()
                    
                    emcurso = str(1)
                    cursor.execute('UPDATE TableOpCola SET "exp" = %s WHERE Seq = %s AND EP = %s', (emcurso, SEQ, EP))
                    conn.commit()

            # Close the connection
            conn.close()

        except (Exception, psycopg2.Error) as error:
            QMessageBox.warning(self,"Erro",f"{error}")

    def group_table_by_columnsExpedicao(self,database_file): #Updates tableOpCola
        
        try:
            # Connect to the database
            conn = psycopg2.connect(database="db", host="localhost", user="postgres", password="teste123", port="5432")
            cursor = conn.cursor()
            query = 'SELECT DISTINCT seq FROM tableexp'
            cursor.execute(query)

            sequencias = cursor.fetchall()
            
            for row in sequencias:
                sequenciaatual = int(row[0])
                
                
                # Execute the query

                query = 'SELECT DISTINCT ep FROM tableexp WHERE seq = %s' 

                cursor.execute(query,(sequenciaatual,))
                
                arows = cursor.fetchall()

                NumeroPeçasExistentes = len(arows)
                
                #print("necessário: " + str(NumeroPeçasColadas))
                cursor.execute('SELECT DISTINCT ep FROM tablelogcorte_c_k WHERE seq = %s',(sequenciaatual,))
                brows = cursor.fetchall()
                
                NumeroPeçasCompletas = len(brows)
                
                #print("atual: " + str(NumeroPeçasCompletas))
                if NumeroPeçasExistentes < NumeroPeçasCompletas:
                    min_qtdpossivel = 0
                else:
                    
                    query = '''SELECT EP, QTDPossivel
                            FROM (
                                SELECT EP, "QTD Fornecida" / CAST(fi AS INTEGER) AS QTDPossivel
                                FROM tableexp
                                WHERE seq = %s
                            ) AS subquery
                            ORDER BY QTDPossivel
                            LIMIT 1;'''
                    cursor.execute(query,(sequenciaatual,))

                    # Fetch the result
                    min_qtdpossivel = int(cursor.fetchone()[1])
                
                
                #print("Quantidade final possivel:" + str(min_qtdpossivel))
                try:
                    # Construct and execute the UPDATE query
                    update_query = 'UPDATE tableexp SET "CJEs a Expedir" = %s WHERE Seq = %s'
                    cursor.execute(update_query, (min_qtdpossivel, sequenciaatual))

                    # Commit the transaction
                    conn.commit()

                except Exception as e:
                    # Print or log any error messages
                    print("Error executing UPDATE query:", e)
                
                conn.commit()
            
                # Execute the query
                
            conn.close()

        except (Exception, psycopg2.Error) as error:
            QMessageBox.warning(self,"Erro",f"{error}")
    


    def ConcludeExpedition(self):
        try:
            selected_row = self.ui.TableExp.currentRow()

            if selected_row >= 0:
                DataProd = self.ui.TableExp.item(selected_row, 0).text()
                CJEAtual = self.ui.TableExp.item(selected_row, 1).text()

                conn = psycopg2.connect(database="db", host="localhost", user="postgres", password="teste123", port="5432")
                cursor = conn.cursor()
                
                cursor.execute('SELECT * FROM tablelogcorte_c_k WHERE "Data Produção" = %s AND cje = %s', (DataProd, CJEAtual))
                rows = cursor.fetchall()
                epsnecessarios = []
                for item in rows:
                    epsnecessarios.append(item[2])
               
                NumeroPeçasColadas = len(rows)

                cursor.execute('SELECT * FROM tableexp WHERE "Data Produção" = %s AND cje = %s', (DataProd, CJEAtual))
                rows = cursor.fetchall()
                
                epsatuais = []
                for item in rows:
                    epsatuais.append(item[2])

                NumeroPeçasCompletas = len(rows)
                CJEsPedidos = float(self.ui.TableExp.item(selected_row, 11).text())
                
                CJEsDisponiveis = float(self.ui.TableExp.item(selected_row, self.ui.TableExp.columnCount() - 2).text())
                
                if NumeroPeçasCompletas >= NumeroPeçasColadas:
                    
                    if CJEsDisponiveis >= CJEsPedidos:
                        ex = 4
                        cursor.execute('UPDATE tableexp SET "exp" = %s WHERE "Data Produção" = %s AND cje = %s', (ex, DataProd, CJEAtual))
                        for row in rows:
                            insert_query = "INSERT INTO expedicao VALUES %s"
                            cursor.execute(insert_query, (row,))
                        conn.commit()
                        QMessageBox.warning(self, "Peças expedidas com sucesso", "O Conjunto " + str(CJEAtual) + " foi expedido com sucesso")
                    else:
                        QMessageBox.warning(self, "Conjuntos em falta", "Faltam " + str(CJEsPedidos - CJEsDisponiveis) + " para poder expedir")

                else:
                    
                    epsfalta = [ep for ep in epsnecessarios if ep not in epsatuais]
                    missing_items_str = ", ".join(map(str, epsfalta))
                    QMessageBox.warning(self, "EPs em falta", "Faltam " + str(NumeroPeçasColadas - NumeroPeçasCompletas) + " Conjuntos para poder expedir: " + missing_items_str)
            else:
                QMessageBox.warning(self, "Linha não selecionada", "Selecione uma linha")

        except (Exception, psycopg2.Error) as error:
            QMessageBox.warning(self,"Erro",f"{error}")


    def UpdateExpedicao(self):
        
        
        try:
            conn = psycopg2.connect(database="db", host="localhost", user="postgres", password="teste123", port="5432")
            cursor = conn.cursor()

            cursor.execute('SELECT MAX(seq) FROM tableexp')
            row = cursor.fetchone()

            rows = int(str(row[0]))
            
            for i in range(1,rows+1):
                cursor.execute('SELECT MAX(secagem) FROM tableexp WHERE seq = %s',(i,))
                temposec = str(cursor.fetchone()[0])
                cursor.execute('UPDATE tableexp SET secagem = %s WHERE seq = %s',(temposec,i))
                conn.commit()
            if rows == 1:
                seq = 1
                cursor.execute('SELECT * FROM tableexp WHERE seq = %s', (seq,))
                sequencia = cursor.fetchall()
                NumeroEPs = len(sequencia)
                #print(NumeroEPs)
                cursor.execute('SELECT * FROM tablelogcorte_c_k WHERE seq = %s', (seq,))
                NumeroTotalEPs = len(cursor.fetchall())
                #print(NumeroTotalEPs)

                cursor.execute('SELECT "CJEs a Expedir" FROM tableexp WHERE seq = %s', (seq,))
                Nropossivel = cursor.fetchone()[0]
                
                cursor.execute('SELECT "Quantidade CJE" FROM tableexp WHERE seq = %s', (seq,))
                Nronecessario = cursor.fetchone()[0]

                cursor.execute('SELECT "secagem" FROM tableexp WHERE seq = %s', (seq,))
                TempoSecagem = cursor.fetchone()[0]
                
                if NumeroEPs < NumeroTotalEPs:
                    cursor.execute('UPDATE tableexp SET exp = 0 WHERE seq = %s', (seq,))
                    
                elif float(Nropossivel) >= float(Nronecessario) and datetime.datetime.now() > TempoSecagem:
                    
                    cursor.execute('UPDATE tableexp SET exp = 2 WHERE seq = %s', (seq,))
                elif float(Nropossivel) >= float(Nronecessario) and datetime.datetime.now() < TempoSecagem:
                    
                    cursor.execute('UPDATE tableexp SET exp = 3 WHERE seq = %s', (seq,))    
                elif float(Nropossivel) <= float(Nronecessario) and datetime.datetime.now() < TempoSecagem:
                    
                    cursor.execute('UPDATE tableexp SET exp = 3 WHERE seq = %s', (seq,)) 
                else:
                    cursor.execute('UPDATE tableexp SET exp = 1 WHERE seq = %s', (seq,))
                    
                conn.commit()

                conn.close()
                QMessageBox.warning(self, "Tabela Atualizada", "Expedição atualizada com sucesso")
            else:
                for i in range(1, rows+1):
                    cursor.execute('SELECT * FROM tableexp WHERE seq = %s', (i,))
                    sequencia = cursor.fetchall()
                    NumeroEPs = len(sequencia)
                    #print(NumeroEPs)
                    cursor.execute('SELECT * FROM tablelogcorte_c_k WHERE seq = %s', (i,))
                    NumeroTotalEPs = len(cursor.fetchall())
                    #print(NumeroTotalEPs)
                    cursor.execute('SELECT "CJEs a Expedir" FROM tableexp WHERE seq = %s', (i,))
                    Nropossivel = cursor.fetchone()[0]
                    cursor.execute('SELECT "Quantidade CJE" FROM tableexp WHERE seq = %s', (i,))
                    Nronecessario = cursor.fetchone()[0]
                    cursor.execute('SELECT "secagem" FROM tableexp WHERE seq = %s', (i,))
                    TempoSecagem = cursor.fetchone()[0]
                    
                    if NumeroEPs < NumeroTotalEPs:
                        cursor.execute('UPDATE tableexp SET exp = 0 WHERE seq = %s', (i,))
                        
                    elif float(Nropossivel) >= float(Nronecessario) and datetime.datetime.now() > TempoSecagem:
                        
                        cursor.execute('UPDATE tableexp SET exp = 2 WHERE seq = %s', (i,))
                    elif float(Nropossivel) >= float(Nronecessario) and datetime.datetime.now() < TempoSecagem:
                        
                        cursor.execute('UPDATE tableexp SET exp = 3 WHERE seq = %s', (i,))    
                    elif float(Nropossivel) <= float(Nronecessario) and datetime.datetime.now() < TempoSecagem:
                        
                        cursor.execute('UPDATE tableexp SET exp = 3 WHERE seq = %s', (i,)) 
                    else:
                        cursor.execute('UPDATE tableexp SET exp = 1 WHERE seq = %s', (i,))
                    
                    conn.commit()

                conn.close()
                
                QMessageBox.warning(self, "Tabela Atualizada", "Expedição atualizada com sucesso")
            self.group_table_by_columnsExpedicao("a")
        except (Exception, psycopg2.Error) as error:
            QMessageBox.warning(self,"Erro",f"{error}")

        conn.close()
        self.update_table_widgets()


    def print_cut_pieces(self):
        try:
            
            selected_row = self.ui.TableOpCorte_2.currentRow()
            table_widget = self.ui.TableOpCorte_2
            table = "tableopcorte"
            if selected_row >= 0:
                qtd_text = self.ui.QtdOpCorte_3.toPlainText()
                nrolote = self.ui.NroLote.toPlainText()

                if qtd_text != "" and nrolote != "" and self.check_lote(nrolote) == 1:
                    qtd_value = int(qtd_text)

                    id_atual = table_widget.item(selected_row, 33).text()

                    # Connect to the database
                    conn = psycopg2.connect(database="db", host="localhost", user="postgres", password="teste123", port="5432")
                    cursor = conn.cursor()

                    cursor.execute('UPDATE {} SET "QTD Cortada" = %s WHERE id = %s'.format(table), (qtd_value, id_atual,))
                    conn.commit()
                    nrolote = "BL202" + str(nrolote)
                    cursor.execute('UPDATE {} SET "lote" = %s WHERE id = %s'.format(table), (nrolote, id_atual,))
                    conn.commit()

                    QMessageBox.warning(self, str(self.ui.QtdOpCorte_3.toPlainText()) + " blocos cortados com sucesso",
                                    "Por favor informe a desmoldagem para indicar as peças conformes")
                    
                    global emcorte
                    if id_atual in emcorte:
                        #print(emcorte)
                        emcorte.remove(id_atual)
                else:
                    # Show an alarm message for invalid input
                    QMessageBox.warning(self, "Quantidade ou lote Inválido", "Por favor, insira valores válidos.")
            else:
                QMessageBox.warning(self, "Linha não selecionada", "Selecione uma linha")

        except (Exception, psycopg2.Error) as error:
            QMessageBox.warning(self,"Erro",f"{error}")

    def AlterarSequencia(self):
        try:
            
            selected_row = self.ui.TableOpCola.currentRow()
            table_widget = self.ui.TableOpCola
            table = "tableopcola"
            if selected_row >= 0:
                qtd_text = self.ui.AlterarSequenciaText.toPlainText()
                id_selecionado = table_widget.item(selected_row,35).text()
                EPselecionado = table_widget.item(selected_row,2).text()
                seqselecionada = table_widget.item(selected_row,29).text()
                if qtd_text != "" and qtd_text.isdigit():
                    qtd_value = int(qtd_text)

                    # Connect to the database
                    conn = psycopg2.connect(database="db", host="localhost", user="postgres", password="teste123", port="5432")
                    cursor = conn.cursor()

                    cursor.execute('UPDATE {} SET seq = %s WHERE id = %s'.format(table), (qtd_value, id_selecionado,))
                    conn.commit()
                    cursor.execute('UPDATE {} SET seq = %s WHERE id = %s'.format(table), (qtd_value, id_selecionado,))
                    conn.commit()
                    cursor.execute('UPDATE tableint SET seq = %s WHERE seq = %s AND EP = %s'.format(table), (qtd_value,seqselecionada,EPselecionado))
                    conn.commit()
                    self.group_table_by_columnsTableInt("database.db")
                    QMessageBox.warning(self,"Alteração de sequência realizada" ,"Sequência alterada com sucesso da Parte Selecionada.")
                    
                    
                else:
                    # Show an alarm message for invalid input
                    QMessageBox.warning(self, "Inválido", "Número de sequência inválido. Por favor, insira a sequência correta.")
            else:
                QMessageBox.warning(self, "Linha não selecionada", "Selecione uma linha")

        except (Exception, psycopg2.Error) as error:
            QMessageBox.warning(self,"Erro",f"{error}")


    def Enviar_para_Excedente(self):
        selected_row = self.ui.TableOpCola.currentRow()
        table_widget=self.ui.TableOpCola
        if selected_row >= 0:
    
            # Connect to the database
            conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")  # Replace with your database file
            cursor = conn.cursor()

            #cursor = conn.cursor()
            table_widget = self.ui.TableOpCola
            id_selecionado = int(table_widget.item(selected_row,table_widget.columnCount()-1).text())
            # Create a new row by copying the selected row's items
            cursor.execute('SELECT exp FROM tableopcola WHERE id = %s',(id_selecionado,))
            atualexp = str(cursor.fetchone()[0])
            
            if atualexp == "1":
                new_row = []
                for col in range(32):
                    item = table_widget.item(selected_row, col)
                    if item is not None:
                        new_item = item.text()
                        new_row.append(new_item)
                    
                    else:
                        new_row.append("")  # Create an empty item for missing data
                
                # Calculate "QTD OK" and "QTD NOK"
                
                
                
            
                qtd_excedentes = float(table_widget.item(selected_row,32).text())
                
                        
                seqatual = table_widget.item(selected_row, 29).text()       
                epatual = table_widget.item(selected_row, 2).text()
                cursor.execute('SELECT * FROM excedentes WHERE EP = %s',(epatual,))
                todas = cursor.fetchall()
                if len(todas) == 0:
                    new_row_exc = []
                    for col in new_row:
                        new_row_exc.append(col)
                    new_row_exc.append(str(1))
                    new_row_exc.append(str(qtd_excedentes))
                    new_row_exc.append(str(0))
                    new_row_exc.append(str(0))
                    row_position = self.ui.TableLogExcedentes_3.rowCount()
                    
                    placeholders = ','.join(['%s' for _ in new_row_exc])

                    cursor.execute('INSERT INTO excedentes VALUES ({})'.format(placeholders), new_row_exc)

                    for j, value in enumerate(new_row_exc):
                        item = QTableWidgetItem(str(value))
                        self.ui.TableLogExcedentes_3.setItem(row_position, j, item)
                else:
                    cursor.execute('SELECT "QTD OK" FROM excedentes WHERE EP = %s',(epatual,))
                    quantidadeAtual = int(cursor.fetchone()[0])
                    quantidadefinal = quantidadeAtual + qtd_excedentes
                    cursor.execute('UPDATE excedentes SET "QTD OK" = %s WHERE EP = %s',(quantidadefinal,epatual))
                    conn.commit()
                    cursor.execute('UPDATE excedentes SET abastecido = 0 WHERE "QTD OK" > 0')
                    conn.commit()
                cursor.execute('UPDATE tableopcola SET exp = 0 WHERE id = %s',(id_selecionado,))
                conn.commit()
                cursor.execute('DELETE FROM tableint WHERE EP = %s AND seq = %s',(epatual,seqatual))
                cursor.execute('UPDATE tableopcola SET "QTD Fornecida" = 0 WHERE id = %s',(id_selecionado,))
                conn.commit()
                self.group_table_by_columnsTableInt("database.postgres")
                QMessageBox.warning(self, "Sucesso", "A quantidade de " + str(qtd_excedentes) + " foi inserida aos excedentes")
                
            else:
                QMessageBox.warning(self, "Linha Indisponível", "A linha que tentou enviar ou não existe ou foi já enviada. Por favor espere que a aplicação atualize.")
            conn.close()
            
        
            
        else:    
           
        
            QMessageBox.warning(self, "Linha não selecionada", "Selecione uma linha")
        conn.close()

    def print_table_data(self):
        # Connect to the database
        conn = conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
        cursor = conn.cursor()

        # Execute a query to fetch the table data
        cursor.execute("SELECT * FROM TableLogCorte WHERE Seq = 1")

        # Fetch all the rows returned by the query
        table_data = cursor.fetchall()

        # Print the table data
        #for row in table_data:
        #    print(row)

        # Close the database connection
        conn.close()

    def PesquisaSeq(self):
        self.populate_table_widget(self.ui.TableReceitas,"TableLogCorte_C_K",[0,3,4,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,24,25,26,27,28,30,31,32,33])
        self.update_table_widgets()


    def PesquisaSeq_3(self):
        self.populate_table_widget(self.ui.TableOpCola,"TableOpCola",[3,4,6,7,8,9,10,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,30,31,33])
        self.update_table_widgets()

    def PesquisaSeq_4(self):
        self.populate_table_widget(self.ui.TableLogCola,"TableBuffer",[5])
        self.update_table_widgets()

    def PesquisaSeq_5(self):
        self.populate_table_widget(self.ui.TableLogCorte_C_K_2,"TableOpCorteExecutado",[1,3,4,7,8,9,10,11,12,13,14,15,16,17,18,19,24,25,26,27,28,30,31,32,34,36,35])
        self.update_table_widgets()

    def PesquisaSeq_6(self):
        self.populate_table_widget(self.ui.TableLogCorte_C_K,"TableLogCorte_C_K",[1,3,5,7,8,9,10,11,12,14,18,19,20,21,22,23,24,25,26,27,28,30,31,33])
        self.update_table_widgets()

    def PesquisaEP(self):
        self.populate_table_widget(self.ui.TableLogExcedentes_3,"excedentes",[0,1,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,23,24,25,26,27,28,29,30,31,32,34,36,35])
        self.update_table_widgets()

    def clear_and_insert_data(self, table_name, excel_file_path):
        
        def sanitize_value(value):
            if pd.isna(value):
                return "NULL"  # Replace NaN with NULL in the SQL query
            elif isinstance(value, str):
                return f"'{value}'"
            elif isinstance(value, pd.Timestamp) and value.hour == 0 and value.minute == 0 and value.second == 0:
                return value.strftime('%Y-%m-%d')
            return str(value)

        try:
            
            # Connect to the database
            conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
            cursor = conn.cursor()
            
            cursor.execute(f"SELECT MAX(seq) FROM {table_name}")
            
            seq = cursor.fetchone()
            
            
            if seq[0] is None:
                max_seq = 0
                
            else:
                
                max_seq = seq[0]
                

            # Read data from the specific Excel file
            
            df = pd.read_excel(excel_file_path, sheet_name="Fluxo Colado")
            
            # Add 500 to each value in column 30
            
            df.iloc[:, 29] = df.iloc[:, 29] + max_seq
            
            

            # Iterate through the rows and insert them into the table
            for index, row in df.iterrows():
                
                values = [sanitize_value(value) for value in row]

                # Construct the SQL INSERT query with values only
                insert_query = "INSERT INTO {} VALUES ({});".format(
                    table_name,
                    ', '.join([str(value) for value in values])
                )
                cursor.execute(insert_query)
                # Construct the SQL INSERT query with values only ( INSERE NO TABLEOPCOLA)
                insert_query = "INSERT INTO {} VALUES ({});".format(
                    "tableopcola",
                    ', '.join([str(value) for value in values])
                )
                cursor.execute(insert_query)

            # Commit the changes and close the cursor and connection
            conn.commit()
            cursor.close()
            conn.close()
            QMessageBox.warning(self,"Sucesso","Novo plano de produção inserido")

        except (Exception, psycopg2.Error) as error:
            QMessageBox.warning(self,"Erro",f"{error}")
              

    def show_password_input(self):
        password, ok = QInputDialog.getText(self, "Password Input", "Enter Password:")
        if ok:
            # User pressed OK in the dialog
            if password == "teste123":
                table = "tablelogcorte_c_k"
                path = "C:\\Users\\Guilherme Rodrigues\\Desktop\\Excel\\Merge.xlsm"
                self.clear_and_insert_data(table,path)
            else:
                QMessageBox.warning(self,"Password Errada","Password Errada")

    def extract_data_to_excel(self):
        # Database connection parameters
    

        try:
            # Connect to the database
            conn = psycopg2.connect(database="db", host="localhost",user="postgres",password="teste123",port="5432")
            cursor = conn.cursor()

            # Your SQL query to retrieve data
            sql_query = "SELECT * FROM expedicao"

            # Execute the query and fetch the results into a DataFrame
            cursor.execute(sql_query)
            records = cursor.fetchall()

            # Get the column names
            colnames = [desc[0] for desc in cursor.description]

            df = pd.DataFrame(records, columns=colnames)

            # Close the cursor and database connection
            cursor.close()
            conn.close()
            file_path = "C:\\Users\\Guilherme Rodrigues\\Desktop\\Expedicao"
            
            # Save the data to an Excel file at the specified path
            df.to_excel(file_path, index=False,engine='openpyxl')
            QMessageBox.warning(self,"Sucesso","Expedições extraídas para o ficheiro de expedição")

        except (Exception, psycopg2.Error) as error:
            QMessageBox.warning(self,"Erro",f"{error}")
    
    def CorteADecorrer(self):
        try:
            selected_row = self.ui.TableOpCorte_2.currentRow()
            table_widget = self.ui.TableOpCorte_2
            
            if selected_row >= 0:
                idtosave = table_widget.item(selected_row, 33).text()
                global emcorte
                emcorte.append(idtosave)
        except (Exception, psycopg2.Error) as error:
            QMessageBox.warning(self,"Erro",f"{error}")

    def onMoveExpClicked(self):
        self.ui.stackedWidget.setCurrentIndex(0)  # Move to page 0 (index of the desired page)

    def onMoveOpColaClicked(self):
        self.ui.stackedWidget.setCurrentIndex(1)  # Move to page 1 (index of the desired page)

    def onMoveLogColaClicked(self):
        self.ui.stackedWidget.setCurrentIndex(2)  # Move to page 2

    def onMoveOpCorteClicked(self):
        self.ui.stackedWidget.setCurrentIndex(3)  # Move to page 3

    def onMoveAbastBufferClicked(self):
        self.ui.stackedWidget.setCurrentIndex(4)  # Move to page 4
    
    def onMoveLogCorteNColadoClicked(self):
        self.ui.stackedWidget.setCurrentIndex(5)  # Move to page 5

    def onMoveLogCorteColadoClicked(self):
        self.ui.stackedWidget.setCurrentIndex(6)  # Move to page 6

    def onMoveReceitasClicked(self):
        self.ui.stackedWidget.setCurrentIndex(7)  # Move to page 7


# Create tables for buffer, glueing station, and cutting station
#create_tables(database_file)

# Read the Excel file and insert data into the buffer table
buffer_table_name = 'buffer'
#read_excel_and_insert_to_database(excel_file, database_file, buffer_table_name)

# Read the Excel file and insert data into the glueing station table
glueing_station_table_name = 'glueing_station'
#read_excel_and_insert_to_database(excel_file, database_file, glueing_station_table_name)

# Read the Excel file and insert data into the cutting station table
cutting_station_table_name = 'cutting_station'
#read_excel_and_insert_to_database(excel_file, database_file, cutting_station_table_name)

# Main application loop
#while True:
    # Your application logic here...
    #time.sleep(1)  # Sleep for a short duration to avoid a busy-wait loop
    
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
















    

