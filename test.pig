REGISTER /Users/rjurney/Software/pig/contrib/piggybank/java/piggybank.jar
DEFINE LENGTH org.apache.pig.piggybank.evaluation.string.LENGTH();

import 'ntf_idf.macro';

reviews = LOAD 'trimmed_reviews.avro' USING AvroStorage();
review_tokens = FOREACH reviews GENERATE business_id, FLATTEN(TOKENIZE(text)) AS token;
ntf_idf_scores = ntf_idf(review_tokens, 'business_id', 'token');
STORE ntf_idf_scores INTO '/tmp/ntf_idf_scores.txt';