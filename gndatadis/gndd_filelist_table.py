from flask_table import Table, Col

class GNFileLogResults(Table):

    classes = ['table', 'table-striped', 'table-sm']
    flid = Col('fileId')
    filename = Col('filename')
    filetype = Col('filetype')
    filedesc = Col('filedesc')
    filedelim = Col('filedelim')
    fileencode = Col('fileencode')
    state = Col('state')
