# Google Compact Language Detector
import gcld3

class LanguageDetector():
    """
    Builds a Spacy NLP Model for language detection use.
    """
    def __init__(self):
        gcld3_model = gcld3.NNetLanguageIdentifier(min_num_bytes=0, 
                                        max_num_bytes=1000)
        self.language_detector_model = gcld3_model

    def detect_language(self, text: str) -> str:
        """ Detect the language of a text."""
        result = self.language_detector_model.FindLanguage(text)
        return(result.language)
