import pandas as pd
from fuzzywuzzy import fuzz
from mrjob.job import MRJob
import pickle

class MRFuzzyMatcher(MRJob):

    def configure_args(self):
        super(MRFuzzyMatcher, self).configure_args()
        self.add_file_arg('--employer_file')
        self.add_file_arg('--english_words')

    def mapper_init(self):
        self.data_df = pd.read_csv(self.options.employer_file)
        self.data_df['group'] = self.data_df['employer'].apply(lambda x: x[0].upper())
        self.key_words = {'incorporated','company','corporation','plc','llc','-old','etf', 'international','wireless','holdings', 
             'integrated','industries','(the)','of','and','limited','hospital','healthcare','management','university','health',
             'services','center','group','school','care','solutions','associates','medical','a','community',
             'for','county','district','services,','home','construction','systems','city','usa','us','in','consulting','department',
             'american','family','group,','insurance','technologies','america','church','public','national','marketing','nursing','financial',
             'search','strategies','networks','trucking','agency','transportation',
             'united', 'college', 'st', 'restaurant', 'new', 'north', 'rehabilitation', 'engineering', 
             'nurse', 'at', 'com', 'enterprises', 'office', 'dental', 'clinic', 'manufacturing', 
             'technology', 'electric', 'schools', 'auto', 'association', 'veterinary', 'communications', 
             'academy', 'valley', 'living', 'heating', 'state', 'animal', 'regional', 'central', 'global', 
             'union', 'law', 'credit', 'products', 'air', 'west', 'south', 'registered', 'learning', 
             'development', 'park', 'therapy', 'technical', 'design', 'designs', 'electrical', 'contractors',
             'productions', 'electric', 'installation', 'installations', 'rehabilitation',
             'consulting', 'engineers', 'banking', 'centers'}
        self.previous_line = set()
        with open(self.options.english_words) as word_file:
            self.english_words = set(word_file.read().split())

    def remove_common_words(self, name):
        # remove any commas
        if ',' in name:
            stripped_name = ''
            for word in name.split(','):
                stripped_name += word
            name = stripped_name
        
        # remove any quotes   
        if name.startswith('"'):
            name = name[1:]
        if name.endswith('"'):
            name = name[:-1]
        
        # remove .com
        if name.endswith('.com'):
            name = name[:-4]

        # remove any common words
        all_tokens = name.split()
        filtered_tokens = []
        for token in all_tokens:
            if token.lower() not in self.key_words:
                filtered_tokens.append(token)
        
        name = " ".join(filtered_tokens)
        return name
    
    def expand_common_words(self, name):
        # remove any commas
        if ',' in name:
            stripped_name = ''
            for word in name.split(','):
                stripped_name += word
            name = stripped_name
        
        # remove any quotes   
        if name.startswith('"'):
            name = name[1:]
        if name.endswith('"'):
            name = name[:-1]
        
        # capitalize only the first letter of each word
        name = self.capitalize_first_letters(name)
        
        # expand common words, pluralize words, and remove 'the'
        all_tokens = name.split()
        standardized_tokens = []
        for token in all_tokens:
            if token.lower() in ['inc', 'inc.']:
                standardized_tokens.append('Incorporated')
            elif token.lower() in ['co', 'co.']:
                standardized_tokens.append('Company')
            elif token.lower() in ['corp.', 'corp']:
                standardized_tokens.append('Corporation')
            elif token.lower() in ['dept.', 'dept']:
                standardized_tokens.append('Department')
            elif token.lower() in ['univ.', 'univ']:
                standardized_tokens.append('University')
            elif token.lower() in ['svc.', 'svc', 'service']:
                standardized_tokens.append('Services')
            elif token.lower() == 'associate':
                standardized_tokens.append('Associates')
            elif token.lower() in ['ltd.', 'ltd']:
                standardized_tokens.append('limited')
            elif token.lower() == '&':
                standardized_tokens.append('and')
            elif token.lower() != 'the':
                standardized_tokens.append(token)
        
        name = " ".join(standardized_tokens)
        return name
    
    def capitalize_first_letters(self, line):
        name = " ".join(w.capitalize() for w in line.split())
        return name
            
    
    def remove_dictionary_words(self, name):
        all_tokens = name.split()
        filtered_tokens = []
        for token in all_tokens:
            if token.lower() not in self.english_words:
                filtered_tokens.append(token)
        name = " ".join(filtered_tokens)

        return name


    def mapper(self, _, line):
        if len(line) > 0 and line != 'employer':
            self.previous_line.add(line)

            # remove any quotes   
            if line.startswith('"'):
                line = line[1:]
            if line.endswith('"'):
                line = line[:-1]

            standard_line = self.expand_common_words(line)
            clean_line = self.remove_common_words(standard_line)

            if len(clean_line) > 0:
                first_letter = clean_line[0]
                letter_data = self.data_df.groupby(['group']).get_group(first_letter.upper())['employer'].tolist()
                data_list = self.data_df['employer'].values.tolist()
                non_dict_line = self.remove_dictionary_words(standard_line)

                for employer in letter_data:
                    if employer != 'employer' and employer not in self.previous_line:
                        # first, run matching on original names
                        # exact matches only, but first expand common abbreviations and deal with plurals
                        standard_employer = self.expand_common_words(employer)
                        standard_score = fuzz.ratio(standard_line, standard_employer)

                        # second, run matching on cleaned names
                        clean_employer = self.remove_common_words(standard_employer)
                        clean_score = fuzz.ratio(clean_line, clean_employer)

                        # third, run matching on non-dictionary words
                        non_dict_employer = self.remove_dictionary_words(standard_employer)
                        if len(non_dict_line) > 0 or len(non_dict_employer) > 0:
                            non_dict_score = fuzz.ratio(non_dict_line, non_dict_employer)
                        else:
                            non_dict_score = -999

                        if clean_score > 80 or non_dict_score > 80:
                            yield [line, employer, standard_line, standard_employer, clean_line, clean_employer, non_dict_line, non_dict_employer], [standard_score, clean_score, non_dict_score]

if __name__ == '__main__':
    MRFuzzyMatcher.run()