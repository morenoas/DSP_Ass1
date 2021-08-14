import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import java.util.List;
import java.util.Properties;

public class NamedEntityRecognitionHandler {
    private final StanfordCoreNLP NERPipeline;
    public NamedEntityRecognitionHandler(){
        Properties props = new Properties();
        props.put("annotators", "tokenize , ssplit, pos, lemma, ner");
        NERPipeline = new StanfordCoreNLP(props);
    }
    private static final String PERSON = "PERSON";
    private static final String LOCATION = "LOCATION";
    private static final String ORGANIZATION = "ORGANIZATION";


    public String getEntities(String review){
        StringBuilder entitiesList = new StringBuilder();
        // create an empty Annotation just with the given text
        Annotation document = new Annotation(review);
        // run all Annotators on this text
        NERPipeline.annotate(document);
        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
        for(CoreMap sentence: sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(CoreAnnotations.TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);

                if(PERSON.equals(ne) || LOCATION.equals(ne) || ORGANIZATION.equals(ne))
                    entitiesList.append("\t-").append(word).append(":").append(ne);
            }
        }
        return entitiesList.toString();
    }

}
