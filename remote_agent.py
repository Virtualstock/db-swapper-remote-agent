#! /usr/bin/env python

class Agent(object):
    DUMP_COMMAND = 'pg_dump -Fc -f "%(outfile)s" "%(db)s"'
    RESTORE_COMMAND = 'pg_restore -c -d "%(db)s" "%(infile)s"'

    def __init__(self, db_name):
        import logging
        self.logger = logging.getLogger(__name__+"("+db_name+")")
        self.db_name = db_name

    def dump_database(self, db_name):
        import subprocess
        import shlex
        import os
        fname = os.tempnam()
        command = self.DUMP_COMMAND % {
                'db': db_name,
                'outfile': fname}
        self.logger.info("Dumping " + self.db_name + " using '"+command+"'")
        ps = subprocess.Popen(shlex.split(command),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        code = ps.wait()
        stdout, stderr = ps.communicate()
        # Not a nice line, but we have to fail if postgres fails.
        assert code == 0, (code, command, stdout, stderr)
        return fname

    def replace_database(self, db_name, fname):
        import subprocess
        import shlex
        import shutil
        import os

        command = self.RESTORE_COMMAND % {
                'db': db_name,
                'infile': fname}
        self.logger.info("Dumping " + self.db_name + " using '"+command+"'")
        ps = subprocess.Popen(shlex.split(command),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        code = ps.wait()
        stdout, stderr = ps.communicate()
        # Not a nice line, but we have to fail if postgres fails.
        assert code == 0, (code, command, stdout, stderr)
        return fname

    def serve(self, port):
        from wsgiref.simple_server import make_server
        def app(environ, start_response):
            method = environ['REQUEST_METHOD'].upper()
            input_stream = environ['wsgi.input']
            content_len = int(environ.get('CONTENT_LENGTH', '0') or '0')
            # TODO: Get db name from url?
            url = environ.get('PATH_INFO')
            if not url.startswith('/db'):
                start_response('400 Bad request', [])
                return []

            if method == 'PUT':
                # Replace the database with the incoming one
                import os
                fname = os.tempnam()
                total_left = content_len
                CHUNK_SIZE = 2048
                with open(fname, 'wb') as f:
                    while total_left > 0:
                        chunk = input_stream.read(min(CHUNK_SIZE, total_left))
                        f.write(chunk)
                        total_left -= len(chunk)
                self.replace_database(self.db_name, fname)
                start_response('204 OK', [])
                return []

            elif method == 'GET':
                # Dump the database out.
                w = start_response('200 OK', [])
                fname = self.dump_database(self.db_name)
                from wsgiref.util import FileWrapper
                f = open(fname, 'rb')
                return FileWrapper(f)

            elif method == 'HEAD':
                # Validation of connection. Just no-op/OK it.
                start_response('200 OK', [])
                return []

            start_response('400 bad request', [])
            return []

        host = ''
        server = make_server(host, port, app)
        print "Serving on %s:%s" % (host, port)
        server.serve_forever()

def usage():
    return """Usage:
    ./remote_agent.py <db name>

    <db name> must be teh name of the database we are serving for.
        """

def main(args):
    if len(args) != 1:
        print usage()
        return
    db_name = args[0]
    port = 8098
    agent = Agent(db_name)
    agent.serve(port)

if __name__ == '__main__':
    import sys
    main(sys.argv[1:])

