#District Datalabs Incubator 2015
#The Synthesizers
#MVP app
#
#Source: http://flask.pocoo.org/docs/0.10/tutorial/setup/#tutorial-setup

import sqlite3
from mvp_db import conn, curs, qry_commit, qry_drop_basic, qry_drop_corrupt, qry_create_basic, qry_create_corrupt
from flask import Flask, request, session, g, redirect, url_for, \
     abort, render_template, flash, Response

from geco.corruptor import CorruptDataSet
from geco.english_class import original_output2, corrupt_output2,\
                               AttrSet, AttrSetM, row_synth, num_org_rec,\
                               to_string, to_corruptor_gf, test_data_corruptor,\
                               from_tdc, to_corruptor_write_io_string

from StringIO import StringIO
from tempfile import TemporaryFile
import tarfile


#configuration
DATABASE = 'basic.db'
DEBUG = True
SECRET_KEY = 'development key'
USERNAME = 'admin'
PASSWORD = 'default'

#create application
app = Flask(__name__)
app.config.from_object(__name__)

#app.config.from_envvar('MVP_SETTINGS', silent=True)

def init_db():
        qry_commit(qry_drop_basic)
        qry_commit(qry_drop_corrupt)
        qry_commit(qry_create_basic)
        qry_commit(qry_create_corrupt)
        conn.commit()
        conn.close()

init_db()

def connect_db():
    return sqlite3.connect(app.config['DATABASE'])

@app.before_request
def before_request():
    g.db = connect_db()

@app.teardown_request
def teardown_request(exception):
    db = getattr(g, 'db', None)
    if db is not None:
        db.close()


#views with routes
#seperate view from routes for later versions
@app.route('/')
def show_entries():
    cur = g.db.execute('select id, name_last, name_first, gender from basic order by name_last desc')
    entries = [dict(id=row[0], name_last=row[1], name_first=row[2], gender=row[3]) for row in cur.fetchall()]
    return render_template('show_entries.html', entries=entries)

@app.route('/add', methods=['POST'])
def add_entry():
    if not session.get('logged_in'):
        abort(401)

        
    g.db.execute('''insert into basic (name_first, name_last, gender) 
        values (?, ?, ?)''',
                 [request.form['name_first'], request.form['name_last'], request.form['gender']])
    g.db.commit()
    # name_middle, address_1, address_2, city, state, zip, phone, email,
    #, ?, ?, ?, ?, ?, ?, ?, ?
    #\
    #             request.form['name_middle'], 
    #             request.form['address_1'], request.form['address_2'],\
    #             request.form['city'], request.form['state'],
    #             request.form['zip'], request.form['phone'],
    #             request.form['email'],
    flash('data entered')
    return redirect(url_for('show_entries'))

@app.route('/<int:entry_id>', methods=['GET'])
def show_record(entry_id):
    cur = g.db.execute("SELECT id, name_last, name_first, gender FROM basic WHERE id = ?", [entry_id])
    entries = [dict(id=row[0], name_last=row[1], name_first=row[2], gender=row[3]) for row in cur.fetchall()]
    return render_template('single_entry.html', entries=entries, entry_id=entry_id)
    
@app.route('/corrupt/add', methods=['POST'])
def corrupt_add():
    cur = g.db.execute("SELECT id, name_last, name_first, gender FROM basic WHERE id = ?", [request.form['entry_id']])
    #Add corrupting here
    entries = [dict(id=row[0], name_last=row[1], name_first=row[2], gender=row[3]) for row in cur.fetchall()]

    g.db.execute('''insert into corrupt (basic_id, name_first, name_last, gender) 
        values (?, ?, ?, ?)''', [request.form['entry_id'], "b", "c", "d"])
    g.db.commit()
    entry_id = float(request.form['entry_id'])
    flash('data corrupted')
    return redirect(url_for('show_corrupt', entry_id=entry_id))

@app.route('/<int:entry_id>/corrupt', methods=['GET'])
def show_corrupt(entry_id):
    #original entr
    cur = g.db.execute("SELECT id, name_last, name_first, gender FROM basic WHERE id = ?", [entry_id])
    entries = [dict(id=row[0], name_last=row[1], name_first=row[2], gender=row[3]) for row in cur.fetchall()]
    #corrupt counterpart
    cur = g.db.execute("SELECT id, name_last, name_first, gender FROM corrupt WHERE id = ?", [entry_id])
    corrupt = [dict(id=row[0], name_last=row[1], name_first=row[2], gender=row[3]) for row in cur.fetchall()]
    return render_template('single_entry.html', entries=entries, corrupt=corrupt, entry_id=entry_id)

@app.route('/original_out')
def new_original():
    b = AttrSet()
    c = AttrSetM()

    base_output_b = list(row_synth(b, num_org_rec/2 ))
    base_output_c = list(row_synth(c, num_org_rec/2 ))
    base_output_b.extend(base_output_c)

    original_io = to_string(base_output_b, b.output().keys())

    corrupt_io = to_corruptor_write_io_string(\
                 from_tdc(\
                 test_data_corruptor.corrupt_records(\
                 to_corruptor_gf(base_output_b))))

    final_output = (('original', original_io), ('corrupt',corrupt_io))

    def gen_out():
        for out in final_output:

            yield Response(out[1],\
                     mimetype="text/csv",\
                     headers={"Content-Disposition":
                              "attachment;filename="+out[0]+".csv"})

    def csv_stream():
        for out in final_output:
            yield (b'--frontier\r\n' 
                b'Content-Type: text/csv\r\n\r\n' + out[1]+ b'\r\n'
                b'Content-Disposition:attachment; filename=' + out[0] + b'.csv\r\n')


    return Response(csv_stream(),\
                     mimetype="multipart/mixed; boundary=frontier") \
            


@app.route('/all_out')
def new_corrupt():
    b = AttrSet()
    c = AttrSetM()

    base_output_b = list(row_synth(b, num_org_rec/2 ))
    base_output_c = list(row_synth(c, num_org_rec/2 ))
    base_output_b.extend(base_output_c)

    original_io = to_string(base_output_b, b.output().keys())

    corrupt_io = to_corruptor_write_io_string(\
                 from_tdc(\
                 test_data_corruptor.corrupt_records(\
                 to_corruptor_gf(base_output_b))))

    final_output = (('original.csv', original_io), ('corrupt.csv',corrupt_io))

    with tarfile.open("synthesized_stream.tar.gz", "w:gz") as tar:
        for output in final_output:
            to_tar = tarfile.TarInfo(output[0])
            to_tar.size =  len(output[1])
            tar.addfile(to_tar, StringIO(output[1]))
        #to_tar = tarfile.TarInfo('geco_log.txt')
        #to_tar.size = test_data_corruptor.corrupt_log.len
        #tar.addfile(test_data_corruptor.corrupt_log.read())

    #tar = tarfile.open("synthesized_stream.tar.gz", "r|gz")
    def tar_stream():
        tar = open("synthesized_stream.tar.gz", "rb")
        yield tar.read()
    
    return Response(tar_stream(),\
                     mimetype="application/gzip",\
                     headers={"Content-Disposition":
                              "attachment;filename=synthesized.tar.gz"})
@app.route('/select_attr/', methods=['POST'])
def select_attr():
    
    num_org_rec = request.form["NumGen"]
    num_dup_rec = request.form["NumDup"]
    max_duplicate_per_record = request.form["MaxDup"]
    max_modification_per_attr = request.form["MaxMod_Attr"]
    num_modification_per_record = request.form["MaxMod_Rec"]
    
    #CorruptDataSet from corruptors.py
    test_data_corruptor = CorruptDataSet(number_of_org_records = \
                                          num_org_rec,
                                          number_of_mod_records = num_dup_rec,
                                          attribute_name_list = attr_name_list,
                                          max_num_dup_per_rec = \
                                                 max_duplicate_per_record,
                                          num_dup_dist = \
                                                 num_duplicates_distribution,
                                          max_num_mod_per_attr = \
                                                 max_modification_per_attr,
                                          num_mod_per_rec = \
                                                 num_modification_per_record,
                                          attr_mod_prob_dict = \
                                                 attr_mod_prob_dictionary,
                                          attr_mod_data_dict = \
                                                 attr_mod_data_dictionary)
    b = AttrSet()
    c = AttrSetM()
    base_output_b = list(row_synth_alt(b, num_org_rec/2 ))
    base_output_c = list(row_synth_alt(c, num_org_rec/2 ))
    base_output_b.extend(base_output_c)

    original_io = to_string(base_output_b, b.output().keys())

    corrupt_io = to_corruptor_write_io_string(\
                 from_tdc(\
                 test_data_corruptor.corrupt_records(\
                 to_corruptor_gf(base_output_b))))

    return str(request.form.values())

#log in log out
@app.route('/login/', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form['username'] != app.config['USERNAME']:
            error = 'Invalid username'
        elif request.form['password'] != app.config['PASSWORD']:
            error = 'Invalid password'
        else:
            session['logged_in'] = True
            flash('You were logged in')
            return redirect(url_for('show_entries'))
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    flash('You were logged out')
    return redirect(url_for('show_entries'))


if __name__ == '__main__':
    app.run()
