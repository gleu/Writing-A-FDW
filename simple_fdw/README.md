simple_fdw
==========

Simple Foreign Data Wrapper

Compilation
-----------

To use this FDW, you first need to compile it. You'll need pg_config and the usual build toolset. Then just launch:

<pre>
make
make install
</pre>

And you're good to go.

Adding the extension
--------------------

Connect to your database, and execute this query:

<pre>
CREATE EXTENSION simple_fdw;
</pre>

