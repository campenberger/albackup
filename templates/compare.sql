WbCreateProfile
  -name="{{ref.name}}" 
  -savePassword=true
  -username="{{ref.db_user}}"
  -password="{{ref.db_password}}"
  -url="jdbc:jtds:sqlserver://{{ref.db_server}}/{{ref.db_name}}"
  -driver=net.sourceforge.jtds.jdbc.Driver;

WbCreateProfile
  -name="{{target.name}}" 
  -savePassword=true
  -username="{{target.db_user}}"
  -password="{{target.db_password}}"
  -url="jdbc:jtds:sqlserver://{{target.db_server}}/{{target.db_name}}"
  -driver=net.sourceforge.jtds.jdbc.Driver;

-- generate a schema difference report
WbSchemaDiff 
  -referenceProfile="{{ref.name}}"
  -targetProfile="{{target.name}}"
  -file="{{cwd}}/diff-{{ref.name}}.xml"
  -includeIndex=true
  -includeProcedures=true
  -includeSequences=true
  -includeTableGrants=true
  -includePrimaryKeys=true
  -includeForeignKeys=true
  -includeViews=true
  -styleSheet="{{sqlwb_dir}}/xslt/wbdiff2html.xslt"
  -xsltOutput="{{cwd}}/diff-{{ref.name}}.html";
