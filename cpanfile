requires 'perl', '5.020';

requires 'DBIO';
requires 'DBIO::PostgreSQL';
requires 'EV::Pg', '0.02';
requires 'Future', '0.49';
requires 'namespace::clean';

on test => sub {
  requires 'Test::More', '0.98';
  requires 'Test::Exception';
  requires 'DBIO::Test';
};
