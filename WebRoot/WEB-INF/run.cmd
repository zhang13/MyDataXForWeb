set CLASSPATH=.;bin
for %%1 in (lib\*.jar) do call setclasspath.bat %%1

set CLASSPATH=%CLASSPATH%;classes;.;
java -Duser.timezone=GMT+08 com.analysis.analysis.Main