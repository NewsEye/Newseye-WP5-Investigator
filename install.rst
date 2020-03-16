Install Instructions for the Investigator
=========================================

Following are the instructions to get investigator to work on your own
computer. Investigator is developed using Python 3.7.

Get Added to NewsEYE Group at UH
--------------------------------

Be sure that you can access NewsEYE servers, e.g.

    ssh username@melkki.cs.helsinki.fi

    username@melkki:~$ ssh username@newseye.cs.helsinki.fi

Clone the Repository and Create Virtual Environment
---------------------------------------------------

    git clone https://version.helsinki.fi/newseye/wp5.git

    cd wp5

    python3.7 -m venv env

    source env/bin/activate

    pip install -r requirements.txt

PyICU from the requirements may need some manual configuration to
install correctly.

PyICU requires libicu-dev Ubuntu package to work => if installing PyICU causes an error message, do

>> sudo apt-get install libicu-dev

and THEN

pip install -r requirements.txt


Summarization requires additional models:
see app/analysis/summarization/readme.txt

python -m spacy download en
python -m spacy download fr
python -m spacy download xx_ent_wiki_sm


Install PostgreSQL
------------------

*Unix*

Do the following terminal commands

    sudo apt-get install postgresql

    sudo -u postgres -i

    psql

Then in psql shell:

    create user username;

    create database newseye_investigator;

    \q

*OSX*

First install `Homebrew <https://brew.sh/>`_ and then do the following
terminal commands:

    brew install postgresql

    createuser username

    createdb newseye_investigator

The ``username`` is advised to be your UH user name.

Create Flask DB Tables
----------------------

Activate virtual environment and upgrade database tables (in the repo
root).

    source env/bin/activate

    flask db upgrade


Add Local Configurations
------------------------

Copy ``investigator_tmpl.env`` as ``investigator.env`` and change
SECRET_KEY to the right one. If you do not have it, ask for it.


Remote Access to SOLR
---------------------

Add into ``~/.ssh/config``:

    Host solr

         HostName newseye.cs.helsinki.fi

         ProxyJump melkki.cs.helsinki.fi


Then in terminal:

    ssh -L [your favorite port]:localhost:9985 solr

[your favorite port] should be the same one which is used in
``investigator.env`` in SOLR_URI (9983 for now).

then go to http://localhost:9983/ and check you can see the database


Create Flask Token
------------------

Inside virtual environment run

    flask shell

Then in the shell run

    from config import Config

    import jwt

    import datetime

    token=jwt.encode({"username":username, "exp":datetime.datetime.utcnow()+datetime.timedelta(days=1)},Config.SECRET_KEY,algorithm="HS256")

    token

where ``username`` is the same name created to the Flask database.


Run Flask (API) Server
----------------------

Using virtual environment, execute in terminal

    flask run

or if you need the debug mode, execute in terminal

    FLASK_DEBUG=1 flask run

then go to http://localhost:5000/docs and check if you can see API
documentation.

Try Flask Server
----------------

You can test the server, e.g. by running the following command

then try a query, e.g.:

    curl --request POST   --url http://localhost:5000/api/analysis/   --header 'authorization: JWT FLASK_TOKEN' --header 'content-type: application/json' --data '{"search_query": {"q": "president"},"utility": "extract_facets","force_refresh": "T"}'

The ``FLASK_TOKEN`` is the one you created.

If the request returns a sensible results, then everything should be
configured correctly.
