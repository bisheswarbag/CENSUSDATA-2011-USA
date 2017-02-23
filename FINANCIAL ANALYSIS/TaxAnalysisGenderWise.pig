data = LOAD '/niit/census/sample.dat' USING PigStorage(',') AS (Age,Education:chararray,maritalstatus:chararray,gender:chararray,TaxFilerStatus:chararray,Income:chararray,Parents:chararray,CountryOfBirth:chararray,Citizenship:chararray,WeeksWorked:chararray);
filtergender = FOREACH data GENERATE $3,$4;
groupbygendertax = GROUP filtergender by (gender,TaxFilerStatus);
TaxAnalysisGenderWise = foreach groupbygendertax GENERATE group AS gender,COUNT(filtergender.TaxFilerStatus) as count;
dump TaxAnalysisGenderWise;
