

PARSE_VIA_LLM_PROMPT="""


Here is the text corpus relevant to our task:
{corpus}

Here is explicit_dict_keywords which should be used for parsing:
{parse_keywords}

Task Description:
Your job is to parse the text into a json format using given explicit_dict_keywords and NOT ANYTHING ELSE. 
Do NOT add or remove or normalize information. ALSO NEVER impute new fields if related corpus dont have them. Your job is parsing. 
IF there is no information regarding any explicit_dict_keywords, you must put it's value as None 

If corpus includes  multiple isolated keyword related content, output a list of dict with given keyword. Omit: Do not generate a key in your output JSON when the source text lacked that key.
ALSO do not add any extra keys or layers other than given explicit_dict_keywords

During parsing only use given designated explicit_dict_keywords 



Give the output in strict JSON format without HTML TAGS

"""



PROMPT_analyze_individual_filter_prompt_experiment= """

You are an expert evaluator analyzing a filter strategy experiment for information extraction. 

**EXPERIMENT DATA:**
```json
{experiment_data}
```

**TARGET EXTRACTION:** The experiment was trying to extract "all information about voltage" from a semiconductor product page.

**EXPECTED VOLTAGE INFORMATION:**
- Reverse voltage specifications (VR)
- Forward voltage specifications (VF) 
- Maximum voltage ratings
- Voltage-related parameters from technical tables

**EVALUATION CRITERIA:**
Analyze this single experiment run based on these 3 criteria:

## 1. RETAINING_TARGET_INFORMATION (33 points)
- Did the filter capture ALL relevant voltage specifications?
- Are both reverse voltage (VR) and forward voltage (VF) specifications present?
- Are voltage-related parameters from parametric tables included?
- Missing critical voltage information = major penalty

## 2. ELIMINATING_NON_RELEVANT_INFORMATION (33 points)  
- How much non-voltage content was included?
- Does it contain current specs, packaging info, or other irrelevant data?
- Is the signal-to-noise ratio good for voltage extraction?
- Excessive irrelevant content = penalty

## 3. PRESERVING_INPUT_DATA (34 points)
- Is the original structure and formatting maintained?
- Are technical specifications presented in proper context?
- Are relationships between parameters preserved (e.g., voltage specs within parametric tables)?
- Is the output coherent and well-organized?

**ANALYSIS FORMAT:**
```
EXPERIMENT ANALYSIS - Strategy: [strategy_name] | Run: [run_id] | Iteration: [iteration]

RETAINING_TARGET_INFORMATION: [Score]/33
[Detailed analysis of what voltage information was captured/missed]

ELIMINATING_NON_RELEVANT: [Score]/33  
[Analysis of relevance filtering quality and noise level]

PRESERVING_INPUT_DATA: [Score]/34
[Evaluation of structure, formatting, and context preservation]

TOTAL SCORE: [Sum]/100

KEY STRENGTHS:
- [List main strengths]

KEY WEAKNESSES:  
- [List main issues]

VERDICT: [One sentence overall assessment]
```

**IMPORTANT NOTES:**
- Be objective and specific in your scoring
- Reference actual content when possible
- Consider this is part of a two-phase extraction system (filter → parse)
- Focus on filtering quality, not parsing requirements
- Compare against the ideal of "perfect voltage information extraction"

Analyze this experiment run now:

"""



PROMPT_analyze_filter_prompt_experiment_overall="""
You are an expert evaluator analyzing the complete results of a filter strategy experiment for information extraction.

**INDIVIDUAL EXPERIMENT ANALYSES:**
{merged_individual_results}

**EXPERIMENT CONTEXT:**
- **Task**: Extract "all information about voltage" from semiconductor product pages
- **Strategies Tested**: liberal, inclusive, contextual, recall
- **Runs Per Strategy**: 10 iterations each (40 total runs)
- **System**: Two-phase extraction (FilterHero → ParseHero)

**YOUR TASK:**
Analyze all individual results and provide a comprehensive comparison of the 4 filter strategies across these criteria:

## EVALUATION CRITERIA:

### 1. RETAINING_TARGET_INFORMATION
- Which strategy most consistently captured ALL voltage specifications?
- Success rate for finding VR (reverse voltage) and VF (forward voltage) specs
- Completeness of parametric data extraction

### 2. ELIMINATING_NON_RELEVANT_INFORMATION  
- Which strategy best filtered out irrelevant content?
- Signal-to-noise ratio analysis
- How much packaging/current/timing specs were incorrectly included?

### 3. PRESERVING_INPUT_DATA
- Which strategy best maintained structure and context?
- Quality of formatting and table preservation
- Coherence and organization of output

### 4. CONSISTENCY_OVER_RERUN
- Which strategy gave most consistent results across 10 runs?
- Variance in performance, content length, success rates
- Reliability for production use

**ANALYSIS FORMAT:**
```
# FILTER STRATEGY EXPERIMENT - COMPREHENSIVE ANALYSIS

## EXECUTIVE SUMMARY
[2-3 sentence overall conclusion with recommended strategy]

## STRATEGY RANKINGS

### 🥇 BEST OVERALL: [Strategy Name]
**Strengths**: [Key advantages]
**Weaknesses**: [Main limitations]  
**Best Use Case**: [When to use this strategy]

### 🥈 RUNNER-UP: [Strategy Name]
**Strengths**: [Key advantages]
**Weaknesses**: [Main limitations]
**Best Use Case**: [When to use this strategy]

### 🥉 THIRD PLACE: [Strategy Name]  
**Strengths**: [Key advantages]
**Weaknesses**: [Main limitations]
**Best Use Case**: [When to use this strategy]

### 🚫 WORST PERFORMER: [Strategy Name]
**Major Issues**: [Critical problems]
**Why It Failed**: [Root cause analysis]

## DETAILED CRITERIA ANALYSIS

### RETAINING_TARGET_INFORMATION
**Winner**: [Strategy] - [Why it won]
**Rankings**: 1.[Strategy] 2.[Strategy] 3.[Strategy] 4.[Strategy]
**Key Insights**: [Important patterns observed]

### ELIMINATING_NON_RELEVANT_INFORMATION
**Winner**: [Strategy] - [Why it won]  
**Rankings**: 1.[Strategy] 2.[Strategy] 3.[Strategy] 4.[Strategy]
**Key Insights**: [Important patterns observed]

### PRESERVING_INPUT_DATA
**Winner**: [Strategy] - [Why it won]
**Rankings**: 1.[Strategy] 2.[Strategy] 3.[Strategy] 4.[Strategy]  
**Key Insights**: [Important patterns observed]

### CONSISTENCY_OVER_RERUN
**Winner**: [Strategy] - [Why it won]
**Rankings**: 1.[Strategy] 2.[Strategy] 3.[Strategy] 4.[Strategy]
**Key Insights**: [Important patterns observed]

## PRODUCTION RECOMMENDATIONS

### DEFAULT STRATEGY: [Strategy Name]
**Reasoning**: [Why this should be the default]

### ALTERNATIVE SCENARIOS:
- **High-accuracy critical extraction**: Use [Strategy]
- **Speed-critical processing**: Use [Strategy]
- **Experimental/research use**: Use [Strategy]

## KEY FINDINGS
1. [Most important discovery]
2. [Second most important discovery]  
3. [Third most important discovery]

## ACTIONABLE IMPROVEMENTS
1. [Specific recommendation for improving the system]
2. [Second improvement suggestion]
3. [Third improvement suggestion]
```

**INSTRUCTIONS:**
- Be data-driven and reference specific patterns from the individual analyses
- Provide clear, actionable recommendations
- Consider real-world production use cases
- Be objective about trade-offs between strategies
- Highlight any surprising or counterintuitive findings

Analyze the complete experiment results now:


"""



PROPMT_filter_via_llm_INCLUSIVE="""


  Your task is to identify and extract all potentially relevant content from the source material.
    
**SOURCE MATERIAL:**
{corpus}

**WHAT WE'RE LOOKING FOR:**
{thing_to_extract}

**APPROACH:**
• Include any content that MIGHT be related to the criteria above
• When in doubt, include it rather than exclude it
• Preserve the original text exactly as written
• Include surrounding context that provides meaning
• Cast a wide net - it's better to include extra content than miss something important

**HANDLING MULTIPLE SECTIONS:**
• If you find multiple relevant sections, separate them with "---"
• Include sections even if they only partially match the criteria

**OUTPUT:**
Return all potentially relevant content. If absolutely nothing relates to the criteria, return "NO_CONTENT_FOUND".
        



"""



PROPMT_filter_via_llm_liberal= """

Extract all content that could be relevant to our needs. Err on the side of inclusion.

            **SOURCE:**
            {corpus}

            **TARGET:**
            {thing_to_extract}

            **STRATEGY:**
            ✓ Include anything that might match - even partially
            ✓ When uncertain, include it
            ✓ Keep original text unchanged  
            ✓ Include generous context around matches
            ✓ Better to over-include than under-include

            **FORMAT:**
            - Multiple sections separated by "---"
            - Return "NO_CONTENT_FOUND" only if truly nothing relates

            **BIAS:** Favor inclusion over exclusion.

"""




PROPMT_filter_via_llm_contextual= """

You are a context-aware information extractor. Find content related to the target criteria while preserving complete, meaningful structures.

**SOURCE:**
{corpus}

**TARGET:**
{thing_to_extract}

**CONTEXT-PRESERVATION RULES:**
• Find content sections that relate to the target criteria
• Keep complete structural units intact (full tables, complete lists, entire sections)
• When a table has relevant columns, include the entire table structure  
• When a specification appears in a list, include the complete list context
• Preserve hierarchical relationships (headers, subheadings, table structures)
• Include sufficient context for understanding each finding

**STRUCTURAL INTELLIGENCE:**
• If specifications appear in a parameters table, include the full table
• If specifications are in a features list, include the complete list
• Maintain the semantic relationships between related specifications
• Avoid fragmenting information that belongs together

**OUTPUT:**
• Separate major content sections with "---"
• Preserve original formatting and structure  
• Keep related information unified

**PHILOSOPHY:**
Extract complete, well-contextualized sections that preserve the relationships needed for understanding.

"""


PROPMT_filter_via_llm_recall="""
Scan the content below and extract ALL sections that relate to our target criteria. Be generous with inclusion.

        **CONTENT:**
        {corpus}

        **LOOKING FOR:**
        {thing_to_extract}

        **EXTRACTION RULES:**
        • Include any section that mentions, describes, or relates to the target criteria
        • When in doubt between including vs excluding → INCLUDE
        • Keep original text exactly as written
        • Provide adequate context around each section
        • Separate multiple findings with "---"

        **RECALL PRIORITY:** 
        Your goal is comprehensive coverage. Missing relevant content is worse than including extra content.

        **OUTPUT:**
        All potentially relevant sections, or "NO_CONTENT_FOUND" if nothing relates.
        
     
"""



PROPMT_filter_via_llm_base = """Here is the text corpus relevant to our task:
                            {corpus}

                            Here is the information we are interested in:
                            {thing_to_extract}

                            Task Description:
                            Your job is to filter all relevant information from the provided corpus according to the criteria above.
                            The output should be a text corpus containing the filtered piece(s), preserving their original wording.
                            """