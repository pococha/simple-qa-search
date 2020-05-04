from datetime import datetime
import pickle
from functools import lru_cache

import pymysql
import pandas as pd
from tqdm import tqdm
import numpy as np

class DbProcess:
    # TODO class project
    def __init__(self, db_config, np_path, db_name):
        self.db_config = db_config
        self.np_path = np_path
        self.db_name = db_name
    
    def sql_connect(self):
        host, user, password, database = self.db_config
        return pymysql.connect(host = host, user = user, password = password,
                              database = database)
    
    def __format_input_for_np(self, dic_list_with_vec):
        ret = {}
        for dic in dic_list_with_vec:
            kb_name = dic['kb_name']
            vector = dic['vector']
            if not kb_name in ret:
                ret[kb_name] = []
            ret[kb_name].append(vector) # add dic to corresponding kb_name
        return ret
    
    def __format_delete_for_np(self, dic_list_with_vec):
        ret = {}
        for dic in dic_list_with_vec:
            kb_name = dic['kb_name']
            temp = {}
            temp['question_set_index'] = dic['question_set_index']
            temp['raw_question'] = dic['raw_question']
            if not kb_name in ret:
                ret[kb_name] = []
            ret[kb_name].append(temp) # add dic to corresponding kb_name
        return ret
    
    def __format_input_for_sql(self, dic_list_with_vec, source):
        ret = []
        for dic in dic_list_with_vec:
            temp = [dic['kb_name'], dic['question'], dic['answer'],
                    dic['raw_question'], dic['question_set_index'],
                    source]
            ret.append(temp)
        return ret
    
    def __format_delete_for_sql(self, dic_list_with_vec):
        ret = []
        for dic in dic_list_with_vec:
            temp = [dic['kb_name'], dic['raw_question'],
                    dic['question_set_index']]
            ret.append(temp)
        return ret
    
    def get_question_answer_by(self, kb_name, question_set_index):
        con = self.sql_connect()
        try:
            with con.cursor() as cur:
                sql_query = f'select answer from {self.db_name} where \
                kb_name = %s and question_set_index= %s'
                cur.execute(sql_query, (kb_name, question_set_index))
                row = cur.fetchone()
                answer = row[0]
        finally:
            con.close
        return answer
    
    def get_sql_data_index(self, question_dic):
        ret = {}
        for kb_name in question_dic.keys():
            sql_records = self.sql_get(kb_name)
            for dic in question_dic[kb_name]:
                raw_question = dic['raw_question']
                question_set_index = dic['question_set_index']
                for index, sql_dic in enumerate(sql_records):
                    if kb_name == sql_dic['kb_name'] and \
                    raw_question == sql_dic['raw_question'] and \
                    question_set_index == sql_dic['question_set_index']:
                        break
                if not kb_name in ret:
                    ret[kb_name] = []
                ret[kb_name].append(index)
        return ret
    
    def check_sql_exist(self, kb_name, raw_question, question_set_index):
        con = self.sql_connect()
        ret = False
        try:
            with con.cursor() as cur:
                sql_query = f'select * from {self.db_name} where kb_name = %s \
                and raw_question = %s and question_set_index= %s'
                cur.execute(sql_query, (kb_name, raw_question, question_set_index))
                rows = cur.fetchone()
                if rows:
                    ret = True
        finally:
            con.close()
        return ret
    
    def remove_exists(self, dic_list):
        ret = []
        for dic in dic_list:
            kb_name, raw_question, question_set_index = dic['kb_name'], \
            dic['raw_question'], dic['question_set_index']
            if not self.check_sql_exist(kb_name, raw_question,
                                          question_set_index):
                ret.append(dic)
        return ret
    
    def remove_non_exists(self, dic_list):
        ret = []
        for dic in dic_list:
            kb_name, raw_question, question_set_index = dic['kb_name'], \
            dic['raw_question'], dic['question_set_index']
            if self.check_sql_exist(kb_name, raw_question,
                                          question_set_index):
                ret.append(dic)
        return ret
    
    def add_data(self, dic_list_with_vec, source):
        # mysql db
        if source == 0: # 覆盖原有kb 以新上传为准
            kb_name = dic_list_with_vec[0]['kb_name']
            self.empty_data([kb_name])
        else:
            dic_list_with_vec = self.remove_exists(dic_list_with_vec) # remove duplicates
        count = len(dic_list_with_vec)
        sql_input = self.__format_input_for_sql(dic_list_with_vec, source)
        self.sql_add(sql_input)
        np_input = self.__format_input_for_np(dic_list_with_vec)
        self.np_add(np_input)
        return len(dic_list_with_vec)
    
    def empty_data(self, kb_names):
        ret = []
        try:
            for kb_name in kb_names:
                self.sql_empty(kb_name)
                deletes = self.np_empty(kb_name)
                ret.append(deletes)
        except:
            # TODO if no kb_exists already
            pass
        return ret
    
    def get_data(self, kb_names):
        ret = []
        for kb_name in kb_names:
            kb_vectors = self.np_get(kb_name)
            kb_dic_list = self.sql_get(kb_name)
            for index, dic in enumerate(kb_dic_list):
                dic['vector'] = kb_vectors[index]
            ret.extend(kb_dic_list)
        return ret
    
    def delete_data(self, dic_list):
        dic_list = self.remove_non_exists(dic_list)
        sql_delete_list = self.__format_delete_for_sql(dic_list)
        question_dic = self.__format_delete_for_np(dic_list)
        kb_with_index = self.get_sql_data_index(question_dic)
        self.sql_delete(sql_delete_list)
        self.np_delete(kb_with_index)
        return len(dic_list)
    
    def sql_add(self, sql_input):
        con = self.sql_connect()
        try:
            with con.cursor() as cur:
                sql_query = f'insert into {self.db_name}(kb_name, question, \
                answer, raw_question, question_set_index, source) values \
                (%s, %s, %s, %s, %s, %s)'
                cur.executemany(sql_query, sql_input)
                con.commit()
        finally:
            con.close()
    
    def sql_delete(self, sql_delete_list):
        con = self.sql_connect()
        try:
            with con.cursor() as cur:
                for delete_data in sql_delete_list:
                    sql_query = f'delete from {self.db_name} where kb_name \
                     = %s and raw_question = %s and question_set_index = %s'
                    cur.execute(sql_query, tuple(delete_data))
                con.commit()
        finally:
            con.close()
    
    def sql_get(self, kb_name):
        ret = []
        con = self.sql_connect()
        try:
            with con.cursor() as cur:
                sql_query = f'select * from {self.db_name} where kb_name=%s'
                cur.execute(sql_query, kb_name)
                rows = cur.fetchall()
                for row in rows:
                    temp = {}
                    temp['kb_name'] = row[1]
                    temp['question'] = row[2]
                    temp['answer'] = row[3]
                    temp['raw_question'] = row[4]
                    temp['question_set_index'] = row[5]
                    temp['source'] = row[6]
                    ret.append(temp)
        finally:
            con.close()
        return ret
    
    def sql_empty(self, kb_name):
        con = self.sql_connect()
        try:
            with con.cursor() as cur:
                sql_query = f'delete from {self.db_name} where kb_name = %s'
                cur.execute(sql_query, kb_name)
                con.commit()
        finally:
            con.close()
            
    def np_add(self, np_input):
        # input dic{kb_name:vectors}
        for kb_name in np_input.keys():
            cur_vectors = np.array(np_input[kb_name])
            try:
                kb_vectors = np.load(f'{self.np_path}/{kb_name}.pkl', allow_pickle = True)
            except:
                kb_vectors = np.empty((0, 768), float)
            kb_vectors = np.append(kb_vectors, cur_vectors, axis = 0)
            kb_vectors.dump(f'{self.np_path}/{kb_name}.pkl')
    
    def np_delete(self, kb_with_index):
        for kb_name in kb_with_index.keys():
            indexes = kb_with_index[kb_name]
            kb_vectors = np.load(f'{self.np_path}/{kb_name}.pkl', allow_pickle = True)
            kb_vectors = np.delete(kb_vectors, indexes, axis = 0)
            kb_vectors.dump(f'{self.np_path}/{kb_name}.pkl')
    
    def np_get(self, kb_name):
        try:
            kb_vectors = np.load(f'{self.np_path}/{kb_name}.pkl', allow_pickle = True)
        except FileNotFoundError:
            print(f'kb_name {kb_name} not found')
            kb_vectors = []
        finally:
            return kb_vectors

    def np_empty(self, kb_name):
        # empty kb np array
        kb_vectors = np.load(f'{self.np_path}/{kb_name}.pkl', allow_pickle = True)
        deletes = len(kb_vectors)
        vectors = np.empty((0, 768), float)
        vectors.dump(f'{self.np_path}/{kb_name}.pkl')
        return deletes

if __name__ == '__main__':
    NP_PATH = './np_array'
    db = DbProcess(db_config = ['47.93.81.67','root',
                    'App0926Magic!','mr'],
                   np_path = NP_PATH,
                  db_name = 'qa_db')


# def upload_data(kb_name, dic_list_with_vec, np_path):
#     ''' update data into excel'''
#     # mysql connection
# #     if db == 0:
#     con = pymysql.connect(host='47.93.81.67',user='root',password='App0926Magic!',database='mr')
# #     elif db == 1:
# #         con = pymysql.connect(host='rm-2ze4r9e02kp961m5hno.mysql.rds.aliyuncs.com', user='root',
# #                               password = 'Magics1213Corp!', database='mr')
    
#     # 覆盖 原 kb_name 所有问题
#     try:
#         with con.cursor() as cur:
#             cur.execute('delete from qa_db where kb_name=%s', kb_name)
#     except:
#         pass
    
#     # retreive all vectors and save
#     all_vectors = np.array([x['vector'] for x in dic_list_with_vec])
#     all_vectors.dump(f'{np_path}/{kb_name}.pkl')
#     insert_list = []
#     for dic in dic_list_with_vec:
#         insert_list.append([kb_name, dic['raw_question'], dic['question'], dic['answer'], dic['question_set_index'], 0])
        
#     try:
#         with con.cursor() as cur:
#             temp = 'insert into qa_db(kb_name, raw_question, question, answer, question_set_index, \
#             source) values (%s, %s, %s, %s, %s, %s)'
#             cur.executemany(temp, insert_list)
#             con.commit()
#     finally:
#         con.close()

#     n = len(dic_list_with_vec) # number of question been updated
#     return n

# def read_data_from_db(kb_names, np_path):
#     ''' read and return data from sql db'''
#     ret = []
# #     print(indexes)
#     now = datetime.now()
#     for kb_name in kb_names:
#         all_vectors = np.load(f'{np_path}/{kb_name}.pkl', allow_pickle = True)
#         print(len(all_vectors))
#         con = pymysql.connect(host='47.93.81.67',user='root',password='App0926Magic!',database='mr')
#         try:
#             with con.cursor() as cur:
#                 now = datetime.now()
#                 cur.execute('select * from qa_db where kb_name=%s', kb_name)
#                 print(datetime.now() - now)
#                 rows = cur.fetchall()
#                 print(datetime.now() - now)
#                 for ind, row in tqdm(enumerate(rows)):
#                     temp = {}
#                     temp['kb_name'] = row[0]
#                     temp['question'] = row[1]
#                     temp['answer'] = row[2]
#                     temp['raw_question'] = row[3]
#                     temp['question_set_index'] = row[4]
#                     temp['source'] = row[5]
#                     temp['vector'] = all_vectors[ind]

#                     ret.append(temp)

#         finally:
#             con.close()
#     print(datetime.now() - now)
#     return ret

# def delete_db_records_by(dic_list, np_path):
#     ''' delete records'''
#     count = 0
#     con = pymysql.connect(host='47.93.81.67',user='root',password='App0926Magic!',database='mr')
#     for dic in dic_list:
#         kb_name = dic['kb_name']
#         question_set_index = dic['question_set_index']
#         question = dic['question'].strip()
#         found = 0
#         try:
#             with con.cursor() as cur:
#                 now = datetime.now()
#                 cur.execute('select raw_question from qa_db where kb_name = %s and question_set_index = %s', (kb_name, question_set_index))
#                 rows = cur.fetchall()
#                 for ind, row in enumerate(rows):
#                     if row[0] == question:
#                         count += 1
#                         index = ind
#                         temp = (kb_name, question_set_index, question)
#                         cur.execute('delete from qa_db where kb_name = %s and question_set_index = %s and raw_question = %s', temp)
#                         found = 1
#                         break
#         except:
#             pass
#         if found:
#             all_vectors = np.load(f'{np_path}/{kb_name}.pkl', allow_pickle = True)
#             all_vectors = np.delete(all_vectors, index, axis = 0)
#             all_vectors.dump(f'{np_path}/{kb_name}.pkl')
#     con.commit()
#     con.close()
#     return count

# def get_question_answer_by(kb_name, question_set_index):
#     ''' retrive question answer by kb_name and question_set_index'''
#     print(kb_name)
#     print(question_set_index)
#     con = pymysql.connect(host='47.93.81.67',user='root',password='App0926Magic!',database='mr')
#     try:
#         with con.cursor() as cur:
#             now = datetime.now()
#             cur.execute('select answer from qa_db where kb_name = %s and question_set_index= %s', (kb_name, question_set_index))
#             row = cur.fetchone()
#             answer = row[0]
#     finally:
#         con.close()
#     return answer

# def empty_kb_questions(kb_names, np_path):
#     ''' empty requested kb names '''
#     count = []
#     for kb_name in kb_names:
#         con = pymysql.connect(host='47.93.81.67',user='root',password='App0926Magic!',database='mr')
#         try:
#             with con.cursor() as cur:
#                 now = datetime.now()
#                 cur.execute('delete from qa_db where kb_name=%s', kb_name)
#                 con.commit()
#         finally:
#             con.close()
        
#         all_vectors = np.load(f'{np_path}/{kb_name}.pkl', allow_pickle = True)
#         count.append(len(all_vectors))
#         all_vectors = np.array([])
#         all_vectors.dump(f'{np_path}/{kb_name}.pkl')
#     return count

# def check_if_record_exists(kb_name, question, question_set_index):
#     ''' check if a record already exists in database'''
#     con = pymysql.connect(host='47.93.81.67',user='root',password='App0926Magic!',database='mr')
#     ret = False
#     try:
#         with con.cursor() as cur:
#             temp = 'select * from qa_db where kb_name = %s and question = %s and question_set_index= %s'
#             cur.execute(temp, (kb_name, question, question_set_index))
#             rows = cur.fetchall()
#             if rows:
#                 ret = True
#     finally:
#         con.close()
#     return ret

# def add_questions_to_question_set(dic_list, np_path):
#     ''' add questions to question set according to question set index'''
#     updates = 0
#     con = pymysql.connect(host='47.93.81.67',user='root',password='App0926Magic!',database='mr')
#     for dic in dic_list:
# #         print(dic)
#         updates += 1
#         kb_name = dic['kb_name']
#         question = dic['question']
#         answer = dic['answer']
#         question_set_index = dic['question_set_index']
#         raw_question = dic['raw_question']
#         source = dic['source']
#         if check_if_record_exists(kb_name, question, question_set_index):
#             updates -= 1
#             continue
#         insert_list = [kb_name, raw_question, question, answer, question_set_index, source]
#         try:
#             with con.cursor() as cur:
#                 temp = 'insert into qa_db(kb_name, raw_question, question, answer, question_set_index, \
#                 source) values (%s, %s, %s, %s, %s, %s)'
#                 cur.execute(temp, insert_list)
#         except:
#             print('Data not inserted')
#         all_vectors = np.load(f'{np_path}/{kb_name}.pkl', allow_pickle = True)
#         all_vectors = np.append(all_vectors,[dic['vector']], axis = 0)
#         all_vectors.dump(f'{np_path}/{kb_name}.pkl')
#     con.commit()
#     con.close()
#     return updates
