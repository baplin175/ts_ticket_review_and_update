# Matcha API Documentation

*Generated on 3/27/2026*

---

## Table of Contents
#### Last Updated: 2026-02-10

- [Authentication](#auth_section_header)
- [Chat Completion](#completion_section_header)
- [LLM Passthrough APIs](#openai_section_header)
- [Generate Summary](#summary_section_header)
- [Legacy API Documentation](#legacy_section_header)
- [Matcha Objects](#object_section_header)
- [File Upload](#upload_section_header)
- [Folder Creation](#folder_section_header)
- [File CRUD Operations](#file_crud_section_header)
- [File Summary](#summarize_section_header)
- [File/Folder Analysis](#analyze_section_header)
- [Recompute file records](#recompute_section_header)

---

---

# 1. Authentication

To access the Matcha API, you need to authenticate using an API key. The API key should be included in the request headers as follows:

```
MATCHA-API-KEY: your_api_key_here
```

You can obtain your API key from the Matcha dashboard under the "API" menu. Make sure to keep your API key secure and do not share it publicly.

Example of a `curl` command with the API key:
        @keyframes fadeIn {
          from {
            opacity: 0;
            transform: translateY(10px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
      
```
curl https://matcha.harriscomputer.com/rest/api/v1/missions \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY"
```

---

# 2. Chat Completion API Endpoint

The Chat Completion API endpoint allows you to interact with the Matcha AI backend to generate responses based on user input. The responses from this endpoint should be very similar if not identical to the ones received from the Matcha chatbot.

The endpoint is as follows:

```
POST https://matcha.harriscomputer.com/rest/api/v1/completions
```

The request body should be in JSON format.

Example using `curl` command for a very simple chat completion against the globally available "Matcha's Self Serve Chat" mission:

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/v1/completions \
  -H "Content-Type: application/json" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY" \
  -d '{
    "mission_id": 424,
    "input": "How do I log in to Matcha?"
}'
```

**Response**
```
{
  "status": "success",
  "error": "",
  "output": [
    {
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "### **SUMMARY**\nTo log in to Matcha, you’ll use your Harris credentials through the **“Sign in with Harris Account”** button on the Matcha login page. After you sign in, Matcha automatically assigns you to the correct **Business Unit (BU) workspace** based on the **Company Name** in your Harris Active Directory (**Entra ID**) profile.\n\n---\n\n### **DETAILED GUIDANCE**\n\n#### **How to log in to Matcha**\n1. Go to Matcha: **https://matcha.harriscomputer.com/**\n2. On the login page, select **“Sign in with Harris Account”**.\n3. Sign in using your **Harris credentials**.\n\n#### **How your BU Workspace is assigned after login**\n- When you log in using **“Sign in with Harris Account”**, Matcha looks up your **Company Name** in your Harris Active Directory (**Entra ID**) profile.\n- Matcha uses that Company Name value to automatically assign you to the correct **Matcha Business Unit (BU) workspace**.\n\n---\n\n### **KEY TAKEAWAYS**\n- Access Matcha at **https://matcha.harriscomputer.com/**\n- Use the **“Sign in with Harris Account”** button to log in.\n- Log in with your **Harris credentials**.\n- Your **BU workspace** is automatically assigned based on **Company Name** in **Entra ID**.\n\n---\n\n### **MATCHA SUPPORT or FEATURE REQUESTS**\n> For more information, please contact the Matcha team at: [matcha@harriscomputer.com](mailto:matcha@harriscomputer.com)\n\n### **MATCHA Learning: Matcha Academy in Workday Learn**\n> Start your learning today at: [Matcha Academy: Foundations Level](https://wd3.myworkday.com/harriscomputer/learning/program/33708f05c648100153a195d8d5bc0000?type=2d29754fdb8e100008b50ff6bc94003b)\n\n\n![image](data:image/png;base64,...)"
        }
      ]
    }
  ]
}
```

The request body can be more complex, allowing for multiple messages in the conversation. Here is an example of a more complex request against a specific folder (Documents) within the globally available "Matcha's Self Serve Chat" mission:

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/v1/completions \
  -H "Content-Type: application/json" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY" \
  -d '{
    "folder_id": 523,
    "context": "You are helping a new user answer questions about Matcha.",
    "chat_history": [
      {
        "role": "user",
            "content": "Can I make a workspace that contains missions that only I can see?"
        },
        {
            "role": "assistant",
            "content": [
                {
                    "type": "output_text",
                    "text": "Yes, you can create a Private Workspace where missions, folders, and files are only viewable by the creator, which is you."
                }
            ]
        }
    ],
    "messages": [
        {
            "role": "user",
            "content": "How about if I want to share with a small group of people?"
        }
    ]
}'
```

**Response**
```
{
  "status": "success",
  "error": "",
  "output": [
    {
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "### **SUMMARY**\nYes—if you’re working in the **“Presentation Creation”** mission type, Matcha supports **sharing presentation access with specific other users**, which is a good fit for sharing with a small group. Access is controlled so that users will only see files they own or that have been explicitly shared with them.\n\n---\n\n### **DETAILED GUIDANCE**\n\n#### Sharing with a small group (what Matcha supports)\nBased on the features you listed, Matcha includes **Sharing and Collaboration** for **presentations**:\n\n- **Presentation access can be shared with other users in Matcha** (so you can choose a small set of people, rather than everyone).\n- Matcha uses **Row-Level Security (RLS)** for presentations, meaning **only the owner and users who have been granted access** should be able to view them.\n- In a **Presentation Creation** mission, the files/folders experience is designed to show **only**:\n  - files the user owns, or  \n  - files the user has been granted shared access to\n\n#### How this impacts what your small group will see\n- If you share a presentation with Users A, B, and C, then:\n  - **You** will see it because you own it.\n  - **A/B/C** will see it because it was shared with them.\n  - Everyone else should not see it (due to the presentations RLS policy).\n\n#### Quick clarifying questions (so I can point you correctly)\n1. Are you sharing a **“Presentation Creation”** mission/presentation, or a **“General Purpose”** mission?\n2. When you say “share,” do you mean **view-only access** or **co-edit/collaboration** access?\n3. Are the people you want to share with already **users inside Matcha**?\n\n> Sorry I couldn't find the exact step-by-step clicks (where the share button/menu lives) in the information you provided—only that sharing with other users is enabled for presentations.\n\n---\n\n### **KEY TAKEAWAYS**\n- **Small-group sharing is supported for presentations** (Presentation Creation mission type) by sharing access with specific Matcha users.\n- **Row-Level Security (RLS)** is used so only owners and explicitly shared users can access presentations.\n- In Presentation Creation missions, users will only see **owned or shared** files.\n- If you confirm whether this is **Presentation Creation vs General Purpose**, I can tailor guidance to your scenario.\n\n---\n\n### **MATCHA SUPPORT or FEATURE REQUESTS**\n> For more information, please contact the Matcha team at: [matcha@harriscomputer.com](mailto:matcha@harriscomputer.com)\n\n### **MATCHA Learning: Matcha Academy in Workday Learn**\n> Start your learning today at: [Matcha Academy: Foundations Level](https://wd3.myworkday.com/harriscomputer/learning/program/33708f05c648100153a195d8d5bc0000?type=2d29754fdb8e100008b50ff6bc94003b)\n"
        }
      ]
    }
  ]
}
```

## Full Completion Request Object Specification

**mission_id | folder_id | file_id** (integer, required): One of these three ids are required. This establishes the settings and scope for the chat completion llm to use.

**messages** (array, required if `input` is not used): A list of messages comprising your desired prompt.
                  Depending on the model you use, different message types
                  (modalities) are supported, like text, images, and
                  documents (pdf, docx, etc.).

**input** (string, required if `messages` is not used): A single string input that will be used as the prompt.

**context** (string, optional): Any additional context you want to provide the LLM besides
                  what already exists in the mission

**chat_history** (array, optional): An array of messages in the current conversation that will
                  give the LLM context into your prompt.

**icl_folder_ids** (array, optional): An array of Matcha folder ids to include for the current mission. 
                  This will force the base64 content of each file in those folders to be 
                  included in the prompt. This is useful for including very specific 
                  information that you want the LLM to use in its answers.

**stream** (boolean, optional): When set to true, the response will be streamed as it is generated by the LLM.
                      This is useful for providing real-time feedback to the user instead of waiting for 
                      the entire response to be generated. Default: `false`

▶
#### options (object) - Optional Parameters

**Examples using complete documents mode:** Below are examples for all three chat modes showing how to use `includeCompleteDocuments` with folder and file filtering options.

##### Mission-level Chat Example

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/v1/completions \
  -H "Content-Type: application/json" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY" \
  -d '{
    "mission_id": 424,
    "input": "What are the key findings from the Q3 reports?",
    "options": {
      "includeCompleteDocuments": true,
      "shouldIncludeICLFolder": true,
      "foldersToInclude": [456, 789],
      "filesToInclude": [1001, 1002]
    }
}'
```

**Response**
```
{
  "status": "success",
  "error": "",
  "output": [
    {
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "### **SUMMARY**\nI can help summarize the key findings, but I need a bit more context about which “Q3 reports” you mean and where they were generated/stored. The documentation provided here doesn’t include any Q3 report content to extract findings from directly.\n\n---\n\n### **DETAILED GUIDANCE**\nTo pull out “key findings,” I need to know what report set you’re referring to. Could you share:\n\n1. **Which Q3 reports**: Are these Matcha platform reports, Workday reports, financial/operational dashboards, or something else?\n2. **Where you’re viewing them**: In Matcha (which workspace/mission), a PDF, Excel, or a BI tool?\n3. **What you mean by “key findings”**: e.g., top KPIs vs. variances vs. risks/issues vs. action items.\n\nIf you paste the **text**, upload screenshots, or share the **main metrics/tables** from the Q3 reports here, I can:\n- identify major trends (up/down changes),\n- call out biggest variances vs. targets/prior periods,\n- list notable risks/anomalies,\n- draft an exec-ready summary.\n\nSorry I couldn't find the information in the documents.\n\n---\n\n### **KEY TAKEAWAYS**\n- “Q3 reports” could refer to multiple sources—please specify which ones.\n- The provided documentation doesn’t contain Q3 report results to summarize.\n- If you share the report content (text/tables/screenshots), I can extract and summarize the key findings.\n\n---\n\n### **MATCHA SUPPORT or FEATURE REQUESTS**\n> For more information, please contact the Matcha team at: [matcha@harriscomputer.com](mailto:matcha@harriscomputer.com)\n\n### **MATCHA Learning: Matcha Academy in Workday Learn**\n> Start your learning today at: [Matcha Academy: Foundations Level](https://wd3.myworkday.com/harriscomputer/learning/program/33708f05c648100153a195d8d5bc0000?type=2d29754fdb8e100008b50ff6bc94003b)\n"
        }
      ]
    }
  ]
}
```

This mission-level chat uses complete documents mode, includes ICL folder examples, and only processes files 1001 and 1002 from folders 456 and 789.

##### Folder-level Chat Example

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/v1/completions \
  -H "Content-Type: application/json" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY" \
  -d '{
    "folder_id": 523,
    "input": "Summarize the financial data",
    "options": {
      "includeCompleteDocuments": true,
      "shouldIncludeICLFolder": false,
      "filesToInclude": [-1]
    }
  }'
```

**Response**
```
{
  "status": "success",
  "error": "",
  "output": [
    {
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "### **SUMMARY**\nI can help summarize your financial data, but I need to see the data (or know where it lives in Matcha) and what kind of summary you want (high-level KPIs vs. detailed breakdown). Once you share that, I can outline a clear summary format and the key insights to pull out.\n\n---\n\n### **DETAILED GUIDANCE**\nTo summarize accurately, please provide one of the following:\n- Paste the financial table/text here, **or**\n- Upload a screenshot/export, **or**\n- Tell me which **Matcha report/dashboard/mission output** you’re looking at and copy the relevant figures.\n\nAlso, which summary style do you want?\n1) **Executive summary** (revenue, expenses, net income, variances, trends)  \n2) **Budget vs. actual** (by department/program/account)  \n3) **Cash-focused** (cash in/out, burn rate, runway)  \n4) **AR/AP-focused** (aging, overdue, payables coming due)\n\nAnd confirm the basics so I summarize the right scope:\n- **Time period** (e.g., Jan 2026, Q4, YTD)\n- **Entities/funds/departments** included\n- **Currency** and whether amounts are **actuals, budget, forecast**, or mixed\n\nIf you share the data, I’ll return:\n- Key totals (revenue, expense, net)\n- Biggest drivers (top categories up/down)\n- Variance/trend callouts\n- Any anomalies (one-time items, missing lines, outliers)\n\nSorry I couldn't find the information in the documents.\n\n---\n\n### **KEY TAKEAWAYS**\n- I can summarize the data once you share the numbers (paste/upload) or identify the exact Matcha output.\n- Tell me the summary type you want (executive, budget vs actual, cash, AR/AP).\n- Confirm the time period and scope so the summary is accurate.\n- I’ll produce totals, drivers, variances/trends, and notable anomalies.\n\n---\n\n### **MATCHA SUPPORT or FEATURE REQUESTS**\n> For more information, please contact the Matcha team at: [matcha@harriscomputer.com](mailto:matcha@harriscomputer.com)\n\n### **MATCHA Learning: Matcha Academy in Workday Learn**\n> Start your learning today at: [Matcha Academy: Foundations Level](https://wd3.myworkday.com/harriscomputer/learning/program/33708f05c648100153a195d8d5bc0000?type=2d29754fdb8e100008b50ff6bc94003b)\n"
        }
      ]
    }
  ]
}
```

This folder-level chat uses complete documents mode, excludes ICL folder examples, and only processes specific files 1001, 1002, and 1003 from folder 456. Note: `foldersToInclude` is not used in folder-level chat.

##### File-level Chat Example

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/v1/completions \
  -H "Content-Type: application/json" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY" \
  -d '{
    "file_id": 202864,
    "input": "What is this file about?",
    "options": {
      "includeCompleteDocuments": false,
      "shouldIncludeICLFolder": true
    }
  }'
```

**Response**
```
{
  "status": "success",
  "error": "",
  "output": [
    {
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "### **SUMMARY**\nThis file looks like a documentation excerpt for **Langflow’s Projects and Flows** user interface. It explains how to navigate back to the Projects page, create/manage flows inside projects, edit flow details, lock flows to prevent changes, and gives a high-level note on how flows are built and executed.\n\n---\n\n### **DETAILED GUIDANCE**\n\n#### **What the file is about (main topics)**\n1. **Managing flows in projects**\n   - The **Projects page** is the landing page when you launch Langflow.\n   - Projects act like folders to organize related flows.\n   - There is a default project called **Starter Project** where flows are stored unless you create a new project.\n   - You can create a project by clicking **Create new project**.\n   - The Projects page is also where you can manage your projects’ **MCP servers** (as mentioned in the text).\n\n2. **Navigating back to Projects**\n   - After editing a flow, to return to the Projects page:\n     - Click the **project name** or the **Langflow icon** in the Langflow header.\n\n3. **Editing flow details**\n   - Steps described:\n     1. On the **Projects** page, locate the flow you want to edit.\n     2. Click **More**, then select **Edit details**.\n     3. Edit the **Name** and **Description**, then click **Save**.\n\n4. **Locking a flow (preventing changes)**\n   - The file indicates you can lock a flow to prevent changes.\n   - Steps shown (note: the excerpt appears cut off/incomplete, but includes):\n     1. On the **Projects** page, locate the flow you want to lock.\n     2. Click **More**, then select **Edit details**.\n\n5. **Creating and running flows (high-level behavior)**\n   - Creating a flow involves connecting components via edges/ports with specific data types (example given: **message ports** send text strings).\n   - The build process:\n     - The graph build calls each component’s `def_build` function to validate/prepare nodes.\n     - The graph is processed in dependency order.\n     - Nodes are built/executed sequentially, and results pass to dependent nodes.\n   - Running/testing:\n     - Prototype flows can be tested in the **Playground**.\n     - For application development, it references triggering flows via the **Langflow API** and more advanced options (custom dependencies, containerization).\n\n#### **Why the text looks messy**\nThe repeated “CfCookie Preferences” and scattered headings suggest this was likely copied from a web page with UI elements (cookie banner text, navigation, or sidebar items) included.\n\n---\n\n### **KEY TAKEAWAYS**\n- The file is documentation about **Langflow Projects and Flows management**.\n- It explains **how to return to Projects**, **edit flow details**, and **lock flows**.\n- It includes a **high-level description** of how flows are built/executed (dependency order, sequential execution).\n- It mentions **ports/data types** when connecting components and using the **Playground** to test flows.\n- The content appears to be a **web-doc copy** with cookie/UI text mixed in.\n\n---\n\n### **MATCHA SUPPORT or FEATURE REQUESTS**\n> For more information, please contact the Matcha team at: [matcha@harriscomputer.com](mailto:matcha@harriscomputer.com)\n\n### **MATCHA Learning: Matcha Academy in Workday Learn**\n> Start your learning today at: [Matcha Academy: Foundations Level](https://wd3.myworkday.com/harriscomputer/learning/program/33708f05c648100153a195d8d5bc0000?type=2d29754fdb8e100008b50ff6bc94003b)\n\n\n![image](data:image/png;base64,...)"
        }
      ]
    }
  ]
}
```

This file-level chat uses complete documents mode and includes ICL folder examples. The complete content of file 1001 is included automatically. Note: `foldersToInclude` and `filesToInclude` are not used in file-level chat.

##### Comprehensive Example with All Options

This example demonstrates a complete request using all available options, including chat history, reasoning mode, focus mode, and various LLM parameters:

**Request**
```
curl 'https://matcha.harriscomputer.com/rest/api/v1/completions/?label=streaming_chat_response' \
  -H "Content-Type: application/json" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY" \
  --data-raw $'{
    "mission_id": 424,
    "folder_id": -1,
    "file_id": -1,
    "messages": [
      {
        "role": "user",
        "content": "How do I log in?"
      }
    ],
    "context": "",
    "chat_history": [],
    "options": {
      "responseLLM": 24,
      "temperature": 0.1,
      "topK": 40,
      "topP": 0.75,
      "repetitionPenalty": 1.07,
      "seed": 1,
      "responseTimeout": 90,
      "useWebSearch": false,
      "useCodeInterpreter": false,
      "useReasoning": true,
      "reasoningTokenBudgetPercent": 80,
      "record_chat_requests_and_responses": false,
      "includeSources": true,
      "includeUsageMetadata": true,
      "refinePromptPronounsLLM": 35,
      "allowLessRelevance": true,
      "llmChatOnly": false,
      "max_hits": 10,
      "rerankMaxHits": 3,
      "enableFocusMode": true,
      "contextInVectorSearch": false,
      "syscontext": "",
      "foldersToInclude": [],
      "filesToInclude": [],
      "includeCompleteDocuments": false,
      "knn_weight": 100,
      "match_weight": 0,
      "shouldIncludeICLFolder": false,
      "includeChunkMetadataToLLM": false
    },
    "stream": false
  }'
```

**Response**
```
{
  "status": "success",
  "error": "",
  "output": [
    {
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "### **SUMMARY**\nTo access Matcha, you need to log in using your Harris credentials through the "Sign In with Harris Account" option. This method connects your Harris Active Directory profile to Matcha and assigns you to the appropriate Business Unit Workspace based on your company information.\n\n### **DETAILED GUIDANCE**\nHere’s how to log into Matcha step-by-step:\n\n1. **Navigate to Matcha Login**:\n   - Access the Matcha login page. If you have a direct link provided by your organization, use that.\n\n2. **Sign In with Harris Account**:\n   - On the login screen, select the “Sign in with Harris Account” button. This option ensures your Harris credentials are used to authenticate your access.\n\n3. **Authentication Process**:\n   - Enter your Harris username and password when prompted.\n   - Complete any additional security steps, such as MFA (Multi-Factor Authentication), if required.\n\n4. **Workspace Assignment**:\n   - Matcha will automatically assign you to the correct Business Unit Workspace based on the “Company Name” in your Harris Active Directory (Entra ID) profile. This ensures you are placed in the workspace tied to your organization.\n\n5. **Access Your Workspace**:\n   - Once logged in, you should see your assigned Business Unit Workspace. Begin working or explore the tools available in your workspace.\n\n### **KEY TAKEAWAYS**\n- Use the **“Sign In with Harris Account”** button to authenticate and access Matcha.\n- Your Business Unit Workspace is assigned based on your company details in Harris Active Directory.\n- Ensure you have your Harris credentials ready and MFA set up if applicable.\n- Reach out to your organization's IT support if you encounter login issues. \n\nIf you have further questions or experience challenges, let me know—I’m happy to assist!\n\n![image](data:image/png;base64,...)\n\n![image](data:image/png;base64,...)",
          "sources": [
            {
              "file_name": "MatchaAI User Guide 2025-08-27b.pdf",
              "score": "85%",
              "page_number": 9,
              "section_title": "Navigating Matcha ",
              "contents": "Login Access  \nAccess Matcha here, and log in with your Harris credentials.  \ney AZ Sign In with Harris Account",
              "file_id": "",
              "from_clarifications": false,
              "ocr_confidence": 100,
              "scorevalue": 0,
              "skip": false
            },
            {
              "file_name": "MatchaAI User Guide 2025-08-27b.pdf",
              "score": "80%",
              "page_number": 9,
              "section_title": "Navigating Matcha ",
              "contents": "Access Matcha here, and log in with your Harris credentials.  \n",
              "file_id": "",
              "from_clarifications": false,
              "ocr_confidence": 100,
              "scorevalue": 0,
              "skip": false
            },
            {
              "file_name": "MatchaAI User Guide 2025-08-27b.pdf",
              "score": "75%",
              "page_number": 40,
              "section_title": "Appendix A: Frequently Asked Questions ",
              "contents": "How am I assigned to a Business Unit Workspace in Matcha?  \nWhen you log into Matcha using the “Sign in with Harris Account” button, it looks up your “Company Name” in the Harris Active Directory “Entra ID” profile and assigns your Matcha BU workspace based on that information.  \nx Senior Project Manager * Professional Services OP »& a & Overview = Contact Organization LinkedIn Send a quick message > @ !nacall- Free on Apr 10 A) worcnerencey Work hours: 9:00 AM - 4:30 PM © 10:44 AM - Your time zone is 3 hours ahead Contact information 8 Email © Chat G Mobile Work phon © Location Company Remote a) NorthStar Enterprise",
              "file_id": "",
              "from_clarifications": false,
              "ocr_confidence": 100,
              "scorevalue": 0,
              "skip": false
            },
            {
              "file_name": "MatchaAI User Guide 2025-08-27b.pdf",
              "score": "60%",
              "page_number": 9,
              "section_title": "Navigating Matcha ",
              "contents": "Login Access",
              "file_id": "",
              "from_clarifications": false,
              "ocr_confidence": 100,
              "scorevalue": 0,
              "skip": true
            },
            {
              "file_name": "MatchaAI User Guide 2025-08-27b.pdf",
              "score": "55%",
              "page_number": 2,
              "section_title": " - Table 1-1 : row #28",
              "contents": "Login Access ........................................................................................................................................................ 8",
              "file_id": "",
              "from_clarifications": false,
              "ocr_confidence": 100,
              "scorevalue": 0,
              "skip": true
            },
            {
              "file_name": "MatchaAI User Guide 2025-08-27b.pdf",
              "score": "20%",
              "page_number": 9,
              "section_title": "Session Controls Once you’re logged in, notice the Session Controls and menu in the upper right ",
              "contents": "a. You can click the “Hamburger” menu icon next to your name to open the navigation menu, including the sign-out button.",
              "file_id": "",
              "from_clarifications": false,
              "ocr_confidence": 100,
              "scorevalue": 0,
              "skip": true
            },
            {
              "file_name": "MatchaAI User Guide 2025-08-27b.pdf",
              "score": "15%",
              "page_number": 9,
              "section_title": "Session Controls Once you’re logged in, notice the Session Controls and menu in the upper right ",
              "contents": "Workspaces Missions Sign Out",
              "file_id": "",
              "from_clarifications": false,
              "ocr_confidence": 100,
              "scorevalue": 0,
              "skip": true
            },
            {
              "file_name": "MatchaAI User Guide 2025-08-27b.pdf",
              "score": "10%",
              "page_number": 9,
              "section_title": "Session Controls Once you’re logged in, notice the Session Controls and menu in the upper right ",
              "contents": "Search @ @ All Harris Bi Demo BU as. My Private Workspace",
              "file_id": "",
              "from_clarifications": false,
              "ocr_confidence": 100,
              "scorevalue": 0,
              "skip": true
            },
            {
              "file_name": "MatchaAI User Guide 2025-08-27b.pdf",
              "score": "5%",
              "page_number": 17,
              "section_title": "“Type your message” box ",
              "contents": "x",
              "file_id": "",
              "from_clarifications": false,
              "ocr_confidence": 100,
              "scorevalue": 0,
              "skip": true
            },
            {
              "file_name": "MatchaAI User Guide 2025-08-27b.pdf",
              "score": "5%",
              "page_number": 17,
              "section_title": "“Type your message” box ",
              "contents": "oe",
              "file_id": "",
              "from_clarifications": false,
              "ocr_confidence": 100,
              "scorevalue": 0,
              "skip": true
            }
          ]
        }
      ]
    }
  ],
  "usage_metadata": {
    "reranker": {
      "input_tokens": 535,
      "output_tokens": 163,
      "total_tokens": 698,
      "input_token_details": {},
      "output_token_details": {},
      "llm_id": 1
    },
    "response": {
      "input_tokens": 1183,
      "output_tokens": 368,
      "total_tokens": 1551,
      "input_token_details": {
        "audio": 0,
        "cache_read": 0
      },
      "output_token_details": {
        "audio": 0,
        "reasoning": 0
      },
      "llm_id": 24
    }
  }
}
```

This example shows a full-featured request with:

- **Chat history:** Maintains conversation context
- **Reasoning mode:** `useReasoning: true` with 80% token budget
- **Focus mode:** `enableFocusMode: true` for better relevance
- **Pure semantic search:** `knn_weight: 100`, `match_weight: 0`
- **Custom timeout:** 600 seconds for reasoning models
- **Sources and metadata:** Includes file references and token usage

##### Using Options for Sources and Usage Metadata Example

The `options` object allows you to request additional information in the response, such as source file references and token usage statistics. This example shows a request with `includeSources` and `includeUsageMetadata` enabled:

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/v1/completions \
  -H "Content-Type: application/json" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY" \
  -d '{
    "mission_id": 424,
    "input": "What are the main features of Matcha?",
    "stream": true,
    "options": {
      "includeSources": true,
      "includeUsageMetadata": true,
      "knn_weight": 100,
      "match_weight": 0
    }
}'
```

**Response**
```
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"###"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" **"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"SUMMARY"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"**\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"Sorry"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" I"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" couldn't"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" find"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" information"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" in"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" documents"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" provided"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" here"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" to"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" accurately"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" describe"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" main"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" features"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" of"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Match"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"a"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"."}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" The"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" content"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" you"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" pasted"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" looks"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" like"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" a"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" table"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" of"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" contents"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" excerpt"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":","}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" but"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" it"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" doesn"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"’t"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" include"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" actual"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" feature"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" descriptions"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":".\n\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"If"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" you"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" can"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" share"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" text"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" from"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" “"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"Introduction"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"”"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" /"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" “"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"Key"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Concepts"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" in"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Match"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"a"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"”"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" sections"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" ("}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"or"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" a"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" screenshot"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" of"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" those"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" pages"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"),"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" I"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" can"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" summarize"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" main"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" features"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" precisely"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":".\n\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"---\n\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"###"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" **"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"DE"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"TA"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"ILED"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" GUID"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"ANCE"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"**\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"Based"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" on"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" what"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" you"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" shared"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":","}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" document"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" appears"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" to"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" cover"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" these"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" *"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"concept"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" areas"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"*"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" ("}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"but"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" it"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" does"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" not"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" provide"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" details"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" needed"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" to"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" confirm"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" “"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"main"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" features"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"”"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"):\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"-"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" **"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"Work"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"spaces"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"**\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"-"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" **"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"M"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"issions"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"**\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"-"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" **"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"File"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Types"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"**\n\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"To"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" answer"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" your"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" question"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" accurately"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":","}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" please"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" send"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" one"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" of"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" following"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":":\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"1"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"."}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" The"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" actual"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" paragraph"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" text"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" under"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" **"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"“"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"Introduction"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"”"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"**"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" and"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" **"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"“"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"Key"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Concepts"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" in"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Match"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"a"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"”"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"**,"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" or"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"  \n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"2"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"."}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" A"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" screenshot"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"/image"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" of"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" pages"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" where"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" those"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" sections"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" are"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" explained"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":","}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" or"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"  \n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"3"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"."}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" The"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" specific"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" section"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" in"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" document"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" titled"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" **"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"“"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"Features"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"”"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"**"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" ("}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"if"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" it"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" exists"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":")\n\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"Once"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" you"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" share"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" that"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":","}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" I"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"’ll"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" extract"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" and"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" list"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" main"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" features"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" ("}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"and"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" explain"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" what"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" each"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" one"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" does"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":","}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" when"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" to"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" use"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" it"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":","}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" and"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" any"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" best"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" practices"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" included"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" in"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" docs"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":").\n\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"---\n\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"###"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" **"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"KEY"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" TAKE"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"AW"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"AYS"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"**\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"-"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" I"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" can"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"’t"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" confirm"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Match"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"a"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"’s"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" main"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" features"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" from"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" provided"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" excerpt"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" because"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" it"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"’s"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" only"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" a"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" table"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"-of"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"-"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"contents"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" style"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" listing"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":".\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"-"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" The"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" visible"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" topics"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" suggest"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" “"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"Work"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"spaces"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":",”"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" “"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"M"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"issions"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":",”"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" and"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" “"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"File"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Types"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"”"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" are"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" key"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" concepts"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" covered"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" in"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" docs"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":".\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"-"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Share"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" actual"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" section"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" text"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" ("}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"or"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" images"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" of"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" those"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" pages"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":")"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" and"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" I"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"’ll"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" produce"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" an"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" accurate"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" features"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" summary"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" based"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" on"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" documentation"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":".\n\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"---\n\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"###"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" **"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"MATCH"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"A"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" SUPPORT"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" or"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" FEATURE"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" REQUEST"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"S"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"**\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":">"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" For"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" more"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" information"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":","}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" please"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" contact"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" the"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Match"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"a"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" team"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" at"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":":"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" ["}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"match"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"a"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"@"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"h"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"arris"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"computer"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":".com"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"]("}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"mailto"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":":"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"match"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"a"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"@"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"h"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"arris"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"computer"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":".com"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":")\n\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"###"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" **"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"MATCH"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"A"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Learning"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":":"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Match"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"a"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Academy"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" in"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Work"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"day"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Learn"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"**\n"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":">"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Start"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" your"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" learning"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" today"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" at"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":":"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" ["}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"Match"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"a"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Academy"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":":"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Foundations"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":" Level"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"]("}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"https"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"://"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"wd"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"3"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":".my"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"work"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"day"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":".com"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"/h"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"arris"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"computer"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"/"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"learning"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"/program"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"/"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"337"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"08"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"f"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"05"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"c"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"648"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"100"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"153"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"a"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"195"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"d"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"8"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"d"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"5"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"bc"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"000"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"0"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"?"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"type"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"="}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"2"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"d"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"297"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"54"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"f"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"db"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"8"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"e"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"100"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"008"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"b"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"50"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"ff"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"6"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"bc"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"940"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"03"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":"b"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":")"}]}]}
data: {"status":"success","error":"","output":[{"role":"assistant","content":[{"type":"output_text","text":""}]}]}
data: {
  "status": "success",
  "error": "",
  "output": [
    {
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "",
          "sources": [
            {
              "file_name": "MatchaAI User Guide  2025-09-27 V2.pdf",
              "score": "10%",
              "page_number": 2,
              "section_title": " - Table 1-1 : row #27",
              "contents": "Introduction: ............................................................................................................................................................ 3  \nKey Concepts in Matcha .......................................................................................................................................... 4  \nWorkspaces...................................................................................................................................................... 4",
              "file_id": "136197",
              "from_clarifications": false,
              "ocr_confidence": 86.76,
              "scorevalue": 1.6906083,
              "chunk_display_info": {
                "chunk": 30,
                "contents": "Introduction: ............................................................................................................................................................ 3  \nKey Concepts in Matcha .......................................................................................................................................... 4  \nWorkspaces...................................................................................................................................................... 4",
                "metadata": {
                  "page_number": 2,
                  "section_title": " - Table 1-1 : row #27",
                  "coordinates": {
                    "layout_width": 2550,
                    "layout_height": 3300,
                    "system": "PixelSpace",
                    "points": [
                      [150.20833333333334, 241.67708333333357],
                      [150.20833333333334, 288.5520833333336],
                      [2409.3072916666674, 288.5520833333336],
                      [2409.3072916666674, 241.67708333333357]
                    ]
                  }
                }
              },
              "skip": false
            },
            {
              "file_name": "MatchaAI User Guide  2025-09-27 V2.pdf",
              "score": "4%",
              "page_number": 2,
              "section_title": " - Table 1-1 : row #3",
              "contents": "- Key Concepts IN Matcha .............cccssesccsscccsescecesccensccsesceceescsscensesceeeeeeeeseesesceeeeessesceseeeceeasssensessesceeusessesceseesesensesesseeees: FOLCESS 00.0... ce eeeecececececeeeescecececeeecneeeseeeececeseeasesseseeeeesseeaseseeeeeeeeseeeseeeseeseaseeeseeeececeseeseeeseeeseceseasaeeseneeseeeeeeseeeeenees\n- 4: 4",
              "file_id": "136197",
              "from_clarifications": false,
              "ocr_confidence": 86.76,
              "scorevalue": 1.6877762,
              "chunk_display_info": {
                "chunk": 4,
                "contents": "- Key Concepts IN Matcha .............cccssesccsscccsescecesccensccsesceceescsscensesceeeeeeeeseesesceeeeessesceseeeceeasssensessesceeusessesceseesesensesesseeees: FOLCESS 00.0... ce eeeecececececeeeescecececeeecneeeseeeececeseeasesseseeeeesseeaseseeeeeeeeseeeseeeseeseaseeeseeeececeseeseeeseeeseceseasaeeseneeseeeeeeseeeeenees\n- 4: 4",
                "metadata": {
                  "page_number": 2,
                  "section_title": " - Table 1-1 : row #3",
                  "coordinates": {
                    "layout_width": 2550,
                    "layout_height": 3300,
                    "system": "PixelSpace",
                    "points": [
                      [210.8820037841797, 153.82821655273438],
                      [210.8820037841797, 2492.67236328125],
                      [2377.664794921875, 2492.67236328125],
                      [2377.664794921875, 153.82821655273438]
                    ]
                  }
                }
              },
              "skip": false
            },
            {
              "file_name": "MatchaAI User Guide  2025-09-27 V2.pdf",
              "score": "4%",
              "page_number": 2,
              "section_title": " - Table 1-1 : row #1",
              "contents": "- Key Concepts IN Matcha .............cccssesccsscccsescecesccensccsesceceescsscensesceeeeeeeeseesesceeeeessesceseeeceeasssensessesceeusessesceseesesensesesseeees: WOSKSPACEG.........ccccseeccsssccnesceceeecensecsesceeeesceeesessesceeeeeseeeessenceeeeeseeeseseesceeeeeseecessesceeeeesseceseeeceeasessesesseseseasesseseenees\n- 4: 4  \n- Key Concepts IN Matcha .............cccssesccsscccsescecesccensccsesceceescsscensesceeeeeeeeseesesceeeeessesceseeeceeasssensessesceeusessesceseesesensesesseeees: MISSIONS ..........ccececececeeeeeeseeeeccencceeseseececeeeeeeesseseeeeceeeseeeaseseeseeeeeseceseseeseseseseeeseeeececeseeseeeseeeeeeeecesaueneeseeeeeseeeaeeeeeeees\n- 4: 4",
              "file_id": "136197",
              "from_clarifications": false,
              "ocr_confidence": 86.76,
              "scorevalue": 1.68001,
              "chunk_display_info": {
                "chunk": 2,
                "contents": "- Key Concepts IN Matcha .............cccssesccsscccsescecesccensccsesceceescsscensesceeeeeeeeseesesceeeeessesceseeeceeasssensessesceeusessesceseesesensesesseeees: WOSKSPACEG.........ccccseeccsssccnesceceeecensecsesceeeesceeesessesceeeeeseeeessenceeeeeseeeseseesceeeeeseecessesceeeeesseceseeeceeasessesesseseseasesseseenees\n- 4: 4  \n- Key Concepts IN Matcha .............cccssesccsscccsescecesccensccsesceceescsscensesceeeeeeeeseesesceeeeessesceseeeceeasssensessesceeusessesceseesesensesesseeees: MISSIONS ..........ccececececeeeeeeseeeeccencceeseseececeeeeeeesseseeeeceeeseeeaseseeseeeeeseceseseeseseseseeeseeeececeseeseeeseeeeeeeecesaueneeseeeeeseeeaeeeeeeees\n- 4: 4",
                "metadata": {
                  "page_number": 2,
                  "section_title": " - Table 1-1 : row #1",
                  "coordinates": {
                    "layout_width": 2550,
                    "layout_height": 3300,
                    "system": "PixelSpace",
                    "points": [
                      [210.8820037841797, 153.82821655273438],
                      [210.8820037841797, 2492.67236328125],
                      [2377.664794921875, 2492.67236328125],
                      [2377.664794921875, 153.82821655273438]
                    ]
                  }
                }
              },
              "skip": false
            },
            {
              "file_name": "MatchaAI User Guide  2025-09-27 V2.pdf",
              "score": "4%",
              "page_number": 2,
              "section_title": " - Table 1-1 : row #4",
              "contents": "- Key Concepts IN Matcha .............cccssesccsscccsescecesccensccsesceceescsscensesceeeeeeeeseesesceeeeessesceseeeceeasssensessesceeusessesceseesesensesesseeees: Fil TYP@S.........cccsccccssscceseccnsscecesceeeeeseesceeeesceesseseesceeeseeeseseesceeeeeeseeseseesceeeeceeeseseesceeeeessesseeseseseusssesceeseseeeusesseseeeees\n- 4: 4",
              "file_id": "136197",
              "from_clarifications": false,
              "ocr_confidence": 86.76,
              "scorevalue": 1.6910255,
              "chunk_display_info": {
                "chunk": 5,
                "contents": "- Key Concepts IN Matcha .............cccssesccsscccsescecesccensccsesceceescsscensesceeeeeeeeseesesceeeeessesceseeeceeasssensessesceeusessesceseesesensesesseeees: Fil TYP@S.........cccsccccssscceseccnsscecesceeeeeseesceeeesceesseseesceeeseeeseseesceeeeeeseeseseesceeeeceeeseseesceeeeessesseeseseseusssesceeseseeeusesseseeeees\n- 4: 4",
                "metadata": {
                  "page_number": 2,
                  "section_title": " - Table 1-1 : row #4",
                  "coordinates": {
                    "layout_width": 2550,
                    "layout_height": 3300,
                    "system": "PixelSpace",
                    "points": [
                      [210.8820037841797, 153.82821655273438],
                      [210.8820037841797, 2492.67236328125],
                      [2377.664794921875, 2492.67236328125],
                      [2377.664794921875, 153.82821655273438]
                    ]
                  }
                }
              },
              "skip": false
            }
          ]
        }
      ]
    }
  ],
  "resource_links": [
    {
      "url": "https://harriscomputer.sharepoint.com/:v:/r/CIT/CIT%20External%20Documents/Knowledge%20Base%20Article%20Videos/MatchaExplainerVideo20250429.mp4?csf=1&web=1&e=7wkzbN&nav=eyJyZWZlcnJhbEluZm8iOnsicmVmZXJyYWxBcHAiOiJTdHJlYW1XZWJBcHAiLCJyZWZlcnJhbFZpZXciOiJTaGFyZURpYWxvZy1MaW5rIiwicmVmZXJyYWxBcHBQbGF0Zm9ybSI6IldlYiIsInJlZmVycmFsTW9kZSI6InZpZXcifX0%3D",
      "type": "video",
      "title": "Matcha Introduction Video"
    }
  ]
}

data: {
  "status": "success",
  "error": "",
  "output": [
    { "role": "assistant", "content": [{ "type": "output_text", "text": "" }] }
  ],
  "usage_metadata": {
    "reranker": { "llm_id": 60 },
    "response": {
      "input_tokens": 1655,
      "output_tokens": 538,
      "total_tokens": 2193,
      "input_token_details": { "cache_read": 1536 },
      "output_token_details": { "reasoning": 74 },
      "llm_id": 87
    }
  }
}

data: {
  "status": "success",
  "error": "",
  "output": [
    {
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "done"
        }
      ]
    }
  ]
}
```

With `includeSources: true`, the response includes a `sources` content block showing which files were used. With `includeUsageMetadata: true`, the response includes a top-level `usage` object with token counts. The search weights control the balance between semantic ({{knn_weight}}%) and lexical ({{match_weight}}%) search.

##### Example: Sending File Attachments

Send file attachments by adding each one as a separate `messages` entry with `role: "user"`. Each message's `content` is a single object — a plain string for text, `{ type: "image_url", image_url: { url: "data:image/jpeg;base64,..." } }` for images, or `{ type: "file", file: { file_name: "report.pdf", file_data: "...base64..." } }` for PDFs and other files.

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/v1/completions \
  -H "Content-Type: application/json" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY" \
  -d '{
    "mission_id": 1397,
    "messages": [
      {
        "role": "user",
        "content": {
          "type": "image_url",
          "image_url": {
            "url": "data:image/jpeg;base64,/9j/4AAQSkZJRgAB..."
          }
        }
      },
      {
        "role": "user",
        "content": {
          "type": "file",
          "file": {
            "file_name": "report.pdf",
            "file_data": "JVBERi0xLjQ..."
          }
        }
      },
      {
        "role": "user",
        "content": "Summarize the document and describe the image."
      }
    ],
    "stream": false
  }'
```

Each attachment and your text question are separate `messages` array entries — do not combine them into a single message's content array. The text message can be a plain string: `"content": "Your question here"`.

---

# 3. LLM Passthrough APIs (OpenAI, Anthropic & Gemini)

Matcha provides passthrough endpoints that allow you to use standard OpenAI, Anthropic, and Google Gemini API formats while authenticating through your Matcha API key. These endpoints are useful for integrating with tools and libraries that already support these APIs, such as LangChain, LlamaIndex, or custom applications.

When using a mission-specific endpoint, the mission's system context will be automatically prepended to your messages.

## Available Endpoints

| Provider | Endpoint (without mission) | Endpoint (with mission) |
|---|---|---|
| OpenAI | /rest/api/openai/v1/chat/completions/ | /rest/api/mission_[id]/openai/v1/chat/completions/ |
| Anthropic | /rest/api/anthropic/v1/messages/ | /rest/api/mission_[id]/anthropic/v1/messages/ |
| Gemini | /rest/api/gemini/v1beta/models/[model]:generateContent/rest/api/gemini/v1beta/models/[model]:streamGenerateContent | /rest/api/mission_[id]/gemini/v1beta/models/[model]:generateContent/rest/api/mission_[id]/gemini/v1beta/models/[model]:streamGenerateContent |

## Authentication

You can authenticate using your Matcha API key in any of the following ways:

- `OPENAI-API-KEY` header (for OpenAI endpoint)
- `ANTHROPIC-API-KEY` header (for Anthropic endpoint)
- `X-Goog-Api-Key` header (for Gemini endpoint)
- `X-API-KEY` header (works for all)
- `Authorization: Bearer [your_api_key]` header (standard Bearer token)

## OpenAI Examples

### Basic Chat Completion

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/openai/v1/chat/completions/ \
  -H "Content-Type: application/json" \
  -H "OPENAI-API-KEY: $MATCHA_API_KEY" \
  -d '{
    "model": "gpt-4.1",
    "messages": [
      {
        "role": "user",
        "content": "What is Matcha?"
      }
    ]
  }'
```

**Response**
```
{
  "choices": [
    {
      "content_filter_results": {
        "hate": {
          "filtered": false,
          "severity": "safe"
        },
        "protected_material_code": {
          "detected": false,
          "filtered": false
        },
        "protected_material_text": {
          "detected": false,
          "filtered": false
        },
        "self_harm": {
          "filtered": false,
          "severity": "safe"
        },
        "sexual": {
          "filtered": false,
          "severity": "safe"
        },
        "violence": {
          "filtered": false,
          "severity": "safe"
        }
      },
      "finish_reason": "stop",
      "index": 0,
      "logprobs": null,
      "message": {
        "annotations": [],
        "audio": null,
        "content": "**Matcha** is a *type of powdered green tea* that comes from specially grown and processed tea leaves. Unlike regular green tea, where the leaves are steeped and discarded, matcha involves grinding whole tea leaves into a fine, vibrant green powder.\n\nHere’s a quick overview:\n\n**Origin & Cultivation**\n- Originates from Japan.\n- Tea plants for matcha are shade-grown for weeks before harvest to increase chlorophyll, amino acids (especially L-theanine), and flavor.\n- Only the youngest, highest quality leaves are used.\n\n**Processing**\n- After harvesting, leaves are steamed, dried, and the stems and veins are removed.\n- The remaining leaf material is ground into a powder using stone mills.\n\n**Preparation**\n- The powder is whisked with hot water (not boiling) to create a frothy, rich beverage.\n- Used for \"ceremonial matcha\" (traditional Japanese tea ceremonies) and \"culinary matcha\" (added to lattes, smoothies, pastries, etc.).\n\n**Health Benefits**\n- High in antioxidants, especially catechins.\n- Contains caffeine and L-theanine for focused energy.\n\n**Taste**\n- Flavor is rich, grassy, slightly bitter, and sometimes sweet/umami, depending on quality.\n\n**In summary:**  \nMatcha is a uniquely prepared green tea powder with distinctive taste, vibrant color, and health benefits, enjoyed both as a traditional drink and in modern recipes.",
        "function_call": null,
        "refusal": null,
        "role": "assistant",
        "tool_calls": null
      }
    }
  ],
  "created": 1770660570,
  "id": "chatcmpl-D7PzOIvuHLrFwppG9BAqNdovXsgLM",
  "model": "gpt-4.1-2025-04-14",
  "object": "chat.completion",
  "prompt_filter_results": [
    {
      "content_filter_results": {
        "hate": {
          "filtered": false,
          "severity": "safe"
        },
        "jailbreak": {
          "detected": false,
          "filtered": false
        },
        "self_harm": {
          "filtered": false,
          "severity": "safe"
        },
        "sexual": {
          "filtered": false,
          "severity": "safe"
        },
        "violence": {
          "filtered": false,
          "severity": "safe"
        }
      },
      "prompt_index": 0
    }
  ],
  "service_tier": null,
  "system_fingerprint": "fp_7a7fd0eb44",
  "usage": {
    "completion_tokens": 291,
    "completion_tokens_details": {
      "accepted_prediction_tokens": 0,
      "audio_tokens": 0,
      "reasoning_tokens": 0,
      "rejected_prediction_tokens": 0
    },
    "prompt_tokens": 12,
    "prompt_tokens_details": {
      "audio_tokens": 0,
      "cached_tokens": 0
    },
    "total_tokens": 303
  }
}
```

## Example: Streaming with Tool Calling

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/openai/v1/chat/completions/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $MATCHA_API_KEY" \
  -d '{
    "model": "gpt-4.1",
    "messages": [
      {
        "role": "user",
        "content": "What is the weather in San Francisco?"
      }
    ],
    "tools": [
      {
        "type": "function",
        "function": {
          "name": "get_weather",
          "description": "Get the current weather",
          "parameters": {
            "type": "object",
            "properties": {
              "location": {
                "type": "string",
                "description": "The city and state"
              }
            },
            "required": ["location"]
          }
        }
      }
    ],
    "stream": true
  }'
```

**Response (SSE Stream)**
```
data: {
  "id": "chatcmpl-D7Q1OcvNBpG9lMEz8ST6AAl0cmycD",
  "object": "chat.completion.chunk",
  "created": 1770660694,
  "model": "gpt-4.1",
  "choices": [
    {
      "index": 0,
      "delta": {
        "tool_calls": [
          {
            "index": 0,
            "id": "call_AmpzxCBmlTIijRmaPaaBV18J",
            "type": "function",
            "function": {
              "name": "get_weather",
              "arguments": "{\"location\":\"San Francisco, CA\"}"
            }
          }
        ]
      },
      "finish_reason": null
    }
  ]
}

data: {
  "id": "chatcmpl-D7Q1OcvNBpG9lMEz8ST6AAl0cmycD",
  "object": "chat.completion.chunk",
  "created": 1770660694,
  "model": "gpt-4.1",
  "choices": [{ "index": 0, "delta": {}, "finish_reason": "tool_calls" }],
  "usage": {
    "completion_tokens": 18,
    "prompt_tokens": 56,
    "total_tokens": 74,
    "completion_tokens_details": {
      "accepted_prediction_tokens": 0,
      "audio_tokens": 0,
      "reasoning_tokens": 0,
      "rejected_prediction_tokens": 0
    },
    "prompt_tokens_details": { "audio_tokens": 0, "cached_tokens": 0 }
  }
}

data: [DONE]
```

## Example: Mission-Specific Request

When using the mission-specific endpoint, the mission must have API access enabled (set `apiOnlyMission` to true in mission settings). The mission's system context will be automatically prepended to your messages.

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/mission_424/openai/v1/chat/completions/ \
  -H "Content-Type: application/json" \
  -H "OPENAI-API-KEY: $MATCHA_API_KEY" \
  -d '{
    "model": "gpt-4.1",
    "messages": [
      {
        "role": "user",
        "content": "How do I log in?"
      }
    ],
    "temperature": 0.7,
    "max_tokens": 500
  }'
```

**Response**
```
{
  "choices": [
    {
      "content_filter_results": {
        "hate": {
          "filtered": false,
          "severity": "safe"
        },
        "protected_material_code": {
          "detected": false,
          "filtered": false
        },
        "protected_material_text": {
          "detected": false,
          "filtered": false
        },
        "self_harm": {
          "filtered": false,
          "severity": "safe"
        },
        "sexual": {
          "filtered": false,
          "severity": "safe"
        },
        "violence": {
          "filtered": false,
          "severity": "safe"
        }
      },
      "finish_reason": "stop",
      "index": 0,
      "logprobs": null,
      "message": {
        "annotations": [],
        "audio": null,
        "content": "---\n\n### **SUMMARY**\nLogging in to Matcha requires you to access the Matcha login page and use your assigned credentials. I'll walk you through the basic steps and highlight what to do if you run into any issues, based on the official support documentation.\n\n---\n\n### **DETAILED GUIDANCE**\n\n#### **Standard Login Steps**\n1. **Navigate to the Matcha Login Page**\n   - Go to the official Matcha URL provided by your organization or administrator.\n2. **Enter Your Credentials**\n   - Username: Use your assigned Matcha username (often your work email address).\n   - Password: Enter your Matcha password.\n3. **Click “Log In” or “Sign In”**\n   - You’ll be directed to your Matcha dashboard upon successful login.\n\n#### **If You Forgot Your Password**\n- Click the “Forgot Password?” link on the login page.\n- Enter your email address and follow the instructions in the reset email.\n\n#### **Single Sign-On (SSO) Users**\n- If your organization uses SSO (e.g., logging in via Microsoft, Google, or Okta), click the corresponding button and follow the prompts.\n\n#### **Troubleshooting Login Issues**\n- **Incorrect Credentials**: Double-check your email and password for typos.\n- **Account Not Activated**: Ensure you’ve clicked the activation link sent to your email after registration.\n- **Still Having Trouble?**: Contact your organization’s Matcha administrator or reach out to Matcha support directly.\n\n---\n\n### **KEY TAKEAWAYS**\n- Access Matcha through your organization’s provided login URL.\n- Use your assigned credentials (or SSO if enabled).\n- Use “Forgot Password?” if you need to reset your password.\n- Contact your admin or Matcha support if you can’t log in after these steps.\n\n---\n\n### **MATCHA SUPPORT or FEATURE REQUESTS**\n> For more information, please contact the Matcha team at: [matcha@harriscomputer.com](mailto:matcha@harriscomputer.com)\n\n### **MATCHA Learning: Matcha Academy in Workday Learn**\n> Start your learning today at: [Matcha Academy: Foundations Level](https://wd3.myworkday.com/harriscomputer/learning/program/33708f05c648100153a195d8d5bc0000?type=2d29754fdb8e100008b50ff6bc94003b)\n\n---",
        "function_call": null,
        "refusal": null,
        "role": "assistant",
        "tool_calls": null
      }
    }
  ],
  "created": 1770660830,
  "id": "chatcmpl-D7Q3a9ZrjIdsP78RM48NooyGwpAHt",
  "model": "gpt-4.1-2025-04-14",
  "object": "chat.completion",
  "prompt_filter_results": [
    {
      "content_filter_results": {
        "hate": {
          "filtered": false,
          "severity": "safe"
        },
        "jailbreak": {
          "detected": false,
          "filtered": false
        },
        "self_harm": {
          "filtered": false,
          "severity": "safe"
        },
        "sexual": {
          "filtered": false,
          "severity": "safe"
        },
        "violence": {
          "filtered": false,
          "severity": "safe"
        }
      },
      "prompt_index": 0
    }
  ],
  "service_tier": null,
  "system_fingerprint": "fp_b9041e10b4",
  "usage": {
    "completion_tokens": 494,
    "completion_tokens_details": {
      "accepted_prediction_tokens": 0,
      "audio_tokens": 0,
      "reasoning_tokens": 0,
      "rejected_prediction_tokens": 0
    },
    "prompt_tokens": 1065,
    "prompt_tokens_details": {
      "audio_tokens": 0,
      "cached_tokens": 1024
    },
    "total_tokens": 1559
  }
}
```

## Integration Examples

### Python with OpenAI SDK

        @keyframes fadeIn {
          from {
            opacity: 0;
            transform: translateY(10px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
      
```
from openai import OpenAI

client = OpenAI(
    api_key="your_matcha_api_key",
    base_url="https://matcha.harriscomputer.com/rest/api/openai/v1/"
)

response = client.chat.completions.create(
    model="gpt-4.1",
    messages=[
        {"role": "user", "content": "Hello!"}
    ]
)

print(response.choices[0].message.content)
```

### JavaScript/Node.js

        @keyframes fadeIn {
          from {
            opacity: 0;
            transform: translateY(10px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
      
```
const OpenAI = require('openai');

const client = new OpenAI({
  apiKey: process.env.MATCHA_API_KEY,
  baseURL: 'https://matcha.harriscomputer.com/rest/api/openai/v1/'
});

async function main() {
  const completion = await client.chat.completions.create({
    model: 'gpt-4.1',
    messages: [{ role: 'user', content: 'Hello!' }],
  });

  console.log(completion.choices[0].message.content);
}

main();
```

## Request Parameters

**model** (string, required): The model to use for completion (e.g., 'gpt-4.1', 'gpt-3.5-turbo')

**messages** (array, required): Array of message objects with 'role' and 'content' fields. Roles can be 'system', 'user', 'assistant', or 'tool'.

**temperature** (number, optional): Sampling temperature between 0 and 2. Higher values make output more random. Default is 1.

**max_tokens** (integer, optional): Maximum number of tokens to generate in the completion.

**stream** (boolean, optional): If true, stream back partial progress as server-sent events. Default is false.

**tools** (array, optional): Array of tool/function definitions that the model can call. Supports OpenAI function calling format.

**tool_choice** (string | object, optional): Controls which (if any) tool is called. Can be 'none', 'auto', or specify a particular tool.

**top_p** (number, optional): Set how creative or predictable AI responses are. Lower values = focused answers; higher = more variety.

**frequency_penalty** (number, optional): Number between -2.0 and 2.0. Positive values penalize new tokens based on their frequency. Default is 0.

**presence_penalty** (number, optional): Number between -2.0 and 2.0. Positive values penalize new tokens based on whether they appear in the text so far. Default is 0.

## OpenAI Notes

- Tool calls are buffered when streaming to ensure complete function call data is sent together
- Streaming responses include usage metadata in the final chunk
- All standard OpenAI API parameters are supported and passed through to Azure OpenAI

## Anthropic Examples

### Basic Message Request

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/anthropic/v1/messages/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $MATCHA_API_KEY" \
  -d '{
    "model": "claude-sonnet-4-5",
    "max_tokens": 1024,
    "messages": [
      {
        "role": "user",
        "content": "What is the capital of France?"
      }
    ]
  }'
```

**Response**
```
{
  "content": [
    {
      "citations": null,
      "text": "The capital of France is Paris.",
      "type": "text"
    }
  ],
  "id": "msg_0112ZWj852zu3BbgEhmF3S9q",
  "model": "claude-sonnet-4-5-20250929",
  "role": "assistant",
  "stop_reason": "end_turn",
  "stop_sequence": null,
  "type": "message",
  "usage": {
    "cache_creation": {
      "ephemeral_1h_input_tokens": 0,
      "ephemeral_5m_input_tokens": 0
    },
    "cache_creation_input_tokens": 0,
    "cache_read_input_tokens": 0,
    "inference_geo": "not_available",
    "input_tokens": 14,
    "output_tokens": 10,
    "server_tool_use": null,
    "service_tier": "standard"
  }
}
```

### Streaming Request

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/anthropic/v1/messages/ \
  -H "Content-Type: application/json" \
  -H "ANTHROPIC-API-KEY: $MATCHA_API_KEY" \
  -d '{
    "model": "claude-sonnet-4-5",
    "max_tokens": 1024,
    "stream": true,
    "messages": [
      {
        "role": "user",
        "content": "Write a haiku about programming"
      }
    ]
  }'
```

**Response (SSE Stream)**
```
event: message_start
data: {"message": {"id": "msg_01BeDjBzUyv9mZPEARhT1RgQ", "content": [], "model": "claude-sonnet-4-5-20250929", "role": "assistant", "stop_reason": null, "stop_sequence": null, "type": "message", "usage": {"cache_creation": {"ephemeral_1h_input_tokens": 0, "ephemeral_5m_input_tokens": 0}, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0, "input_tokens": 13, "output_tokens": 1, "server_tool_use": null, "service_tier": "standard", "inference_geo": "not_available"}}, "type": "message_start"}

event: content_block_start
data: {"content_block": {"citations": null, "text": "", "type": "text"}, "index": 0, "type": "content_block_start"}

event: content_block_delta
data: {"delta": {"text": "Code", "type": "text_delta"}, "index": 0, "type": "content_block_delta"}

event: content_block_delta
data: {"delta": {"text": " flows", "type": "text_delta"}, "index": 0, "type": "content_block_delta"}

event: content_block_delta
data: {"delta": {"text": " like water", "type": "text_delta"}, "index": 0, "type": "content_block_delta"}

event: content_block_delta
data: {"delta": {"text": "
Bugs", "type": "text_delta"}, "index": 0, "type": "content_block_delta"}

event: content_block_delta
data: {"delta": {"text": " hide", "type": "text_delta"}, "index": 0, "type": "content_block_delta"}

event: content_block_delta
data: {"delta": {"text": " in silent", "type": "text_delta"}, "index": 0, "type": "content_block_delta"}

event: content_block_delta
data: {"delta": {"text": " spaces", "type": "text_delta"}, "index": 0, "type": "content_block_delta"}

event: content_block_delta
data: {"delta": {"text": "
Compile", "type": "text_delta"}, "index": 0, "type": "content_block_delta"}

event: content_block_delta
data: {"delta": {"text": " and", "type": "text_delta"}, "index": 0, "type": "content_block_delta"}

event: content_block_delta
data: {"delta": {"text": " then", "type": "text_delta"}, "index": 0, "type": "content_block_delta"}

event: content_block_delta
data: {"delta": {"text": " pray", "type": "text_delta"}, "index": 0, "type": "content_block_delta"}

event: content_block_stop
data: {"index": 0, "type": "content_block_stop"}

event: message_delta
data: {"delta": {"stop_reason": "end_turn", "stop_sequence": null}, "type": "message_delta", "usage": {"cache_creation_input_tokens": 0, "cache_read_input_tokens": 0, "input_tokens": 13, "output_tokens": 20, "server_tool_use": null}}

event: message_stop
data: {"type": "message_stop"}
```

### Extended Thinking (Claude Sonnet 4)

Claude Sonnet 4 and Claude 4 Opus support extended thinking, allowing the model to reason through complex problems step-by-step before providing a response.

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/anthropic/v1/messages/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $MATCHA_API_KEY" \
  -d '{
    "model": "claude-sonnet-4-5",
    "max_tokens": 16000,
    "thinking": {
      "type": "enabled",
      "budget_tokens": 10000
    },
    "messages": [
      {
        "role": "user",
        "content": "Solve: If a train travels 120 km in 2 hours, then stops for 30 minutes, then travels 90 km in 1.5 hours, what is its average speed for the entire journey?"
      }
    ]
  }'
```

**Response**
```
{
  "content": [
    {
      "signature": "EukECkYICxgCKkBMr5CNvAAzym5kPMtXFV/3zZD9qqLvBMxky3nnjq47cTk36CaDAb3UYzMQrrmOfr3ZtdcZ2HvCaMSz/1BuyTJmEgwyQMUPqo685N1tCJ4aDC+qBsSGUmBDWSK7gCIwtnHQ4ZmgZA68gmXNsHrmZsi9nj3Na0R2hH5Ro3YCufqMulSIFNEbGuNDvf8c/vnRKtADIClvKkBaXYVRzxkptdSYEg4mbk/ywAJr1/3JTtpN3j8k4jOZC9THKf/wrwZWin56r1qLtZRcmsOtNnAlk3YpXu+L/bfh6yjmdpV9fvUcjBI5x+VpFPjUnKYQr3ptrsc7ZfhYGlKwlmQ10+O038XdoLFgOC4vBGP25JP46qvdgzyEokeqNMeKxpgEYDlbvwGkzHIgWz5kNmvTc2UhM1U2YJCgnH1FV4srBBICQ0eUCkUXp7/e01Lno8HVY7rhvQJz+///epslR5CZ2ABOraT6SaAuu/d1ejw+Gv+ryX1h0hH8TcC2P6krJeyCzyUdYLN5d35w/1kZlxqRLfXxJ/sOwrpveaBxnkdyMjlm6CCIQa1n5gKSsMz+w6eZfa4E9XWrq27pIsrEnddLhG/Xd/oJh0mBGUVytxDEzsBnvuiDlcL6qDmJgGmZh7zUCyV1JEspCPQ49u5obJ1RttDw3rJVis6d1bUgeknWaXlPxxcy8rtOgJHxFepvKIdzfGSIiajZzV8T2ZccBHSAePnKVaYZtQp/B1hxxkDClW1N1WL+BGTRH4j3UWto/Tuk7oZaC75zwXtiXfI6zhQaQNKRf0OUh6U+s4sMwFxyGQ5QdQiLCqcYAQ==",
      "thinking": "Let me break down this problem step by step.\n\nGiven information:\n- First segment: 120 km in 2 hours\n- Stop: 30 minutes\n- Second segment: 90 km in 1.5 hours\n\nTo find average speed, I need:\nAverage speed = Total distance / Total time\n\nTotal distance:\n120 km + 90 km = 210 km\n\nTotal time:\n- First segment: 2 hours\n- Stop: 30 minutes = 0.5 hours\n- Second segment: 1.5 hours\n- Total: 2 + 0.5 + 1.5 = 4 hours\n\nAverage speed = 210 km / 4 hours = 52.5 km/h",
      "type": "thinking"
    },
    {
      "citations": null,
      "text": "# Solution: Average Speed Calculation\n\n## Given Information:\n- **Segment 1:** 120 km in 2 hours\n- **Stop:** 30 minutes\n- **Segment 2:** 90 km in 1.5 hours\n\n## Formula:\n**Average Speed = Total Distance ÷ Total Time**\n\n## Calculations:\n\n### Total Distance:\n120 km + 90 km = **210 km**\n\n### Total Time:\n- Segment 1: 2 hours\n- Stop: 30 minutes = 0.5 hours\n- Segment 2: 1.5 hours\n- **Total: 2 + 0.5 + 1.5 = 4 hours**\n\n### Average Speed:\n210 km ÷ 4 hours = **52.5 km/h**\n\n## Answer:\nThe average speed for the entire journey is **52.5 km/h** (or 52½ km/h).\n\n**Note:** The stop time is included in the total time because average speed considers the entire duration of the journey, including stops.",
      "type": "text"
    }
  ],
  "id": "msg_01QJmgs8SqgDs9ceJTbrc2bT",
  "model": "claude-sonnet-4-5-20250929",
  "role": "assistant",
  "stop_reason": "end_turn",
  "stop_sequence": null,
  "type": "message",
  "usage": {
    "cache_creation": {
      "ephemeral_1h_input_tokens": 0,
      "ephemeral_5m_input_tokens": 0
    },
    "cache_creation_input_tokens": 0,
    "cache_read_input_tokens": 0,
    "inference_geo": "not_available",
    "input_tokens": 85,
    "output_tokens": 440,
    "server_tool_use": null,
    "service_tier": "standard"
  }
}
```

### Mission-Specific Request

When using the mission-specific endpoint, the mission must have API access enabled. The mission's system context will be automatically added to your request.

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/mission_424/anthropic/v1/messages/ \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: $MATCHA_API_KEY" \
  -d '{
    "model": "claude-haiku-4-5",
    "max_tokens": 1024,
    "messages": [
      {
        "role": "user",
        "content": "How do I reset my password?"
      }
    ]
  }'
```

**Response**
```
{
  "content": [
    {
      "citations": null,
      "text": "### **SUMMARY**\nI'd like to help you reset your password! Before I provide the specific steps, let me clarify a couple of things so I can give you the most accurate guidance:\n\n- Are you trying to reset your **Matcha platform password** (your login credentials for the Matcha application itself)?\n- Or are you looking to reset a password for a **different system** that you're integrating with Matcha?\n- Are you currently able to access your Matcha account, or are you locked out?\n\nOnce I understand your specific situation, I can walk you through the exact process!\n\n---\n\n### **MATCHA SUPPORT or FEATURE REQUESTS**\n> For more information, please contact the Matcha team at: [matcha@harriscomputer.com](mailto:matcha@harriscomputer.com)\n\n### **MATCHA Learning: Matcha Academy in Workday Learn**\n> Start your learning today at: [Matcha Academy: Foundations Level](https://wd3.myworkday.com/harriscomputer/learning/program/33708f05c648100153a195d8d5bc0000?type=2d29754fdb8e100008b50ff6bc94003b)",
      "type": "text"
    }
  ],
  "id": "msg_01D6yhjUYjqwjDs4buyjNUcQ",
  "model": "claude-haiku-4-5-20251001",
  "role": "assistant",
  "stop_reason": "end_turn",
  "stop_sequence": null,
  "type": "message",
  "usage": {
    "cache_creation": {
      "ephemeral_1h_input_tokens": 0,
      "ephemeral_5m_input_tokens": 0
    },
    "cache_creation_input_tokens": 0,
    "cache_read_input_tokens": 0,
    "inference_geo": "not_available",
    "input_tokens": 1187,
    "output_tokens": 284,
    "server_tool_use": null,
    "service_tier": "standard"
  }
}
```

### Python with Anthropic SDK

        @keyframes fadeIn {
          from {
            opacity: 0;
            transform: translateY(10px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
      
```
import anthropic

client = anthropic.Anthropic(
    api_key="your_matcha_api_key",
    base_url="https://matcha.harriscomputer.com/rest/api/anthropic"
)

message = client.messages.create(
    model="claude-sonnet-4-5,
    max_tokens=1024,
    messages=[
        {"role": "user", "content": "Hello, Claude!"}
    ]
)

print(message.content[0].text)
```

### JavaScript/Node.js with Anthropic SDK

        @keyframes fadeIn {
          from {
            opacity: 0;
            transform: translateY(10px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
      
```
import Anthropic from '@anthropic-ai/sdk';

const client = new Anthropic({
  apiKey: process.env.MATCHA_API_KEY,
  baseURL: 'https://matcha.harriscomputer.com/rest/api/anthropic'
});

async function main() {
  const message = await client.messages.create({
    model: 'claude-sonnet-4',
    max_tokens: 1024,
    messages: [{ role: 'user', content: 'Hello, Claude!' }],
  });

  console.log(message.content[0].text);
}

main();
```

## Anthropic Request Parameters

**model** (string, required): The model to use (e.g., 'claude-sonnet-4', 'claude-haiku-4-5', 'claude-opus-4')

**messages** (array, required): Array of message objects with 'role' and 'content' fields. Roles can be 'user' or 'assistant'.

**max_tokens** (integer, required): Maximum number of tokens to generate in the response.

**system** (string, optional): System prompt to guide the assistant's behavior. Unlike OpenAI, this is a separate parameter, not part of messages.

**stream** (boolean, optional): If true, stream back partial progress as server-sent events. Default is false.

**thinking** (object, optional): Enable extended thinking. Object with 'type': 'enabled' and 'budget_tokens' for the thinking budget. Only for Claude Sonnet 4/Opus 4.

**temperature** (number, optional): Sampling temperature between 0 and 1. Higher values make output more random. Default is 1.

**top_p** (number, optional): Nucleus sampling parameter. Use either temperature or top_p, not both.

**top_k** (integer, optional): Only sample from the top K options for each token.

**tools** (array, optional): Array of tool definitions that the model can call. Uses Anthropic's tool format.

**tool_choice** (object, optional): Controls which tool is called. Can be 'auto', 'any', or specify a particular tool.

## Anthropic Notes

- System messages should use the `system` parameter, not the messages array
- Extended thinking is only available with Claude Sonnet 4 and Claude Opus 4
- Streaming uses event types (message_start, content_block_delta, etc.) rather than OpenAI's chunk format
- The `max_tokens` parameter is required for Anthropic, unlike OpenAI where it's optional
- All standard Anthropic API parameters are supported and passed through to Azure Anthropic

## Google Gemini Examples

### Non-Streaming Generate Content

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/gemini/v1beta/models/gemini-2.0-flash:generateContent \
  -H "Content-Type: application/json" \
  -H "X-Goog-Api-Key: $MATCHA_API_KEY" \
  -d '{
  "contents": [
    {
      "role": "user",
      "parts": [{"text": "What is Generative AI? Please answer in a single sentence."}]
    }
  ]
}'
```

**Response**
```
{
  "automaticFunctionCallingHistory": [],
  "candidates": [
    {
      "avgLogprobs": -0.04017714542501113,
      "content": {
        "parts": [
          {
            "text": "Generative AI is a type of artificial intelligence that can create new content, such as text, images, audio, and video, by learning patterns from existing data.\n"
          }
        ],
        "role": "model"
      },
      "finishReason": "STOP"
    }
  ],
  "modelVersion": "gemini-2.0-flash",
  "responseId": "JyeKaYKeHrTQ_uMPlfHKgA4",
  "usageMetadata": {
    "candidatesTokenCount": 34,
    "candidatesTokensDetails": [
      {
        "modality": "TEXT",
        "tokenCount": 34
      }
    ],
    "promptTokenCount": 13,
    "promptTokensDetails": [
      {
        "modality": "TEXT",
        "tokenCount": 13
      }
    ],
    "totalTokenCount": 47
  }
}
```

### Streaming Generate Content

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/gemini/v1beta/models/gemini-2.0-flash:streamGenerateContent \
  -H "Content-Type: application/json" \
  -H "X-Goog-Api-Key: $MATCHA_API_KEY" \
  -d '{
  "contents": [
    {
      "role": "user",
      "parts": [{"text": "What is Generative AI? Please answer in a single sentence."}]
    }
  ]
}'
```

**Response (SSE Stream)**
```
data: {"candidates": [{"content": {"parts": [{"text": "Gener"}], "role": "model"}}], "modelVersion": "gemini-2.0-flash", "responseId": "2yeKaciCD5Sf7dcPjMLzoQQ", "usageMetadata": {"promptTokenCount": 14, "promptTokensDetails": [{"modality": "TEXT", "tokenCount": 14}], "totalTokenCount": 14}}

data: {"candidates": [{"content": {"parts": [{"text": "ative AI is"}], "role": "model"}}], "modelVersion": "gemini-2.0-flash", "responseId": "2yeKaciCD5Sf7dcPjMLzoQQ", "usageMetadata": {"promptTokenCount": 14, "promptTokensDetails": [{"modality": "TEXT", "tokenCount": 14}], "totalTokenCount": 14}}

data: {
  "candidates": [
    {
      "content": {
        "parts": [
          {
            "text": " a type of artificial intelligence that can create new content, such as text, images"
          }
        ],
        "role": "model"
      }
    }
  ],
  "modelVersion": "gemini-2.0-flash",
  "responseId": "2yeKaciCD5Sf7dcPjMLzoQQ",
  "usageMetadata": {
    "promptTokenCount": 14,
    "promptTokensDetails": [{ "modality": "TEXT", "tokenCount": 14 }],
    "totalTokenCount": 14
  }
}

data: {
  "candidates": [
    {
      "content": {
        "parts": [
          {
            "text": ", audio, and video, based on patterns learned from existing data.\n"
          }
        ],
        "role": "model"
      },
      "finishReason": "STOP"
    }
  ],
  "modelVersion": "gemini-2.0-flash",
  "responseId": "2yeKaciCD5Sf7dcPjMLzoQQ",
  "usageMetadata": {
    "candidatesTokenCount": 35,
    "candidatesTokensDetails": [{ "modality": "TEXT", "tokenCount": 35 }],
    "promptTokenCount": 13,
    "promptTokensDetails": [{ "modality": "TEXT", "tokenCount": 13 }],
    "totalTokenCount": 48
  }
}
```

### Python with Google Generative AI SDK (Non-Streaming)

        @keyframes fadeIn {
          from {
            opacity: 0;
            transform: translateY(10px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
      
```
from google import genai

# Configure client to use Matcha endpoint
client = genai.Client(
    api_key="your_matcha_api_key",
    http_options={
        'base_url': 'https://matcha.harriscomputer.com/rest/api/gemini'
    }
)

response = client.models.generate_content(
    model='gemini-2.0-flash',
    contents='What is the Generative AI? Please answer in a single sentence.'
)

print(response.text)
```

### Python with Google Generative AI SDK (Streaming)

        @keyframes fadeIn {
          from {
            opacity: 0;
            transform: translateY(10px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
      
```
from google import genai

# Configure client to use Matcha endpoint
client = genai.Client(
    api_key="your_matcha_api_key",
    http_options={
        'base_url': 'https://matcha.harriscomputer.com/rest/api/gemini'
    }
)

# Stream the response
response_stream = client.models.generate_content_stream(
    model='gemini-2.0-flash',
    contents='What is the Generative AI? Please answer in a single sentence.'
)

full_response = ""
usage = None
for chunk in response_stream:
    if hasattr(chunk, 'text') and chunk.text:
        full_response += chunk.text
        print(chunk.text, end='', flush=True)
    if hasattr(chunk, 'usage_metadata') and chunk.usage_metadata:
        usage = chunk.usage_metadata

print(f"\n\nFull response: {full_response}")
```

### Python with LangChain (Non-Streaming)

        @keyframes fadeIn {
          from {
            opacity: 0;
            transform: translateY(10px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
      
```
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
import os

# Set your Matcha API key
os.environ["GOOGLE_API_KEY"] = "your_matcha_api_key"

# Configure LangChain to use Matcha endpoint
llm = ChatGoogleGenerativeAI(
    model="gemini-2.0-flash",
    client_options={
        "api_endpoint": "https://matcha.harriscomputer.com/rest/api/gemini"
    },
    transport="rest"
)

messages = [HumanMessage(content="What is 2+2? Give a very brief answer.")]
response = llm.invoke(messages)

print(response.content)
```

### Python with LangChain (Streaming)

        @keyframes fadeIn {
          from {
            opacity: 0;
            transform: translateY(10px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
      
```
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
import os

# Set your Matcha API key
os.environ["GOOGLE_API_KEY"] = "your_matcha_api_key"

# Configure LangChain to use Matcha endpoint
llm = ChatGoogleGenerativeAI(
    model="gemini-2.0-flash",
    client_options={
        "api_endpoint": "https://matcha.harriscomputer.com/rest/api/gemini"
    },
    transport="rest"
)

messages = [HumanMessage(content="What is 2+2? Give a very brief answer.")]

# Stream the response
full_response = ""
for chunk in llm.stream(messages):
    if chunk.content:
        full_response += chunk.content
        print(chunk.content, end='', flush=True)

print(f"\n\nFull response: {full_response}")
```

### Python with Function/Tool Calling

        @keyframes fadeIn {
          from {
            opacity: 0;
            transform: translateY(10px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
      
```
from google import genai

# Configure client to use Matcha endpoint
client = genai.Client(
    api_key="your_matcha_api_key",
    http_options={
        'base_url': 'https://matcha.harriscomputer.com/rest/api/gemini'
    }
)

# Define a function/tool for the model to use
response = client.models.generate_content(
    model='gemini-2.0-flash',
    contents='What is the weather like in San Francisco today?',
    config={
        'tools': [
            {
                'function_declarations': [
                    {
                        'name': 'get_weather',
                        'description': 'Get the current weather for a location',
                        'parameters': {
                            'type': 'object',
                            'properties': {
                                'location': {
                                    'type': 'string',
                                    'description': 'The city and state, e.g. San Francisco, CA'
                                }
                            },
                            'required': ['location']
                        }
                    }
                ]
            }
        ]
    }
)

# Check if model wants to call a function
if response.candidates and response.candidates[0].content.parts:
    part = response.candidates[0].content.parts[0]
    function_call = getattr(part, 'function_call', None)
    if function_call:
        print(f"Function called: {function_call.name}")
        print(f"Arguments: {dict(function_call.args)}")
        # Example output:
        # Function called: get_weather
        # Arguments: {'location': 'San Francisco, CA'}
    else:
        print(f"Response: {response.text}")
```

## Gemini Request Parameters

**model** (string, in url): The model to use (e.g., 'gemini-2.0-flash', 'gemini-pro'). Specified in the URL path, not the request body.

**contents** (string | array, required): The input content for the model. Can be a simple string (e.g., 'What is AI?') or a structured array of content objects with 'role' and 'parts' fields for multi-turn conversations. The structured format supports mixing text, images, and other content types.

**config** (object, optional): Configuration object with generation parameters. All parameters are optional.

### Core Generation Parameters

**config.temperature** (number, optional): Controls randomness. Range 0.0 to 2.0. Lower values make output more focused and deterministic.

**config.maxOutputTokens** (integer, optional): Maximum number of tokens to generate in the response.

**config.topP** (number, optional): Nucleus sampling parameter (0.0 to 1.0). Use either temperature or topP, not both.

**config.topK** (integer, optional): Top-k sampling parameter. Only sample from top k tokens.

**config.stopSequences** (array[string], optional): Array of strings that will stop generation when encountered.

**config.candidateCount** (integer, optional): Number of response variations to generate.

**config.presencePenalty** (number, optional): Penalize tokens that have already appeared. Range -2.0 to 2.0.

**config.frequencyPenalty** (number, optional): Penalize tokens based on their frequency. Range -2.0 to 2.0.

**config.seed** (integer, optional): Random seed for reproducible outputs.

### System & Response Format

**config.systemInstruction** (string, optional): System instruction to guide model behavior. For mission-specific endpoints, this is automatically populated from the mission's system context.

**config.responseMimeType** (string, optional): Output MIME type (e.g., 'application/json' for JSON responses).

**config.responseSchema** (object, optional): JSON schema to enforce structured output format when responseMimeType is 'application/json'.

### Advanced Features

**config.safetySettings** (array, optional): Array of safety setting objects to control content filtering.

**config.tools** (array, optional): Array of tool/function definitions for function calling.

**config.toolConfig** (object, optional): Configuration for tool/function execution behavior.

**config.responseLogprobs** (boolean, optional): Whether to return log probabilities for generated tokens.

**config.logprobs** (integer, optional): Number of top log probabilities to return per token.

## Gemini Notes

- The model name is specified in the URL path, not in the request body
- Use `:generateContent` for non-streaming and `:streamGenerateContent` for streaming responses
- Gemini uses a different message format than OpenAI/Anthropic - messages are in `contents` with `parts` arrays
- Streaming responses adapt to client requirements: SSE format `data: {...}\n\n` for direct SDK usage, or JSON array format `[{...}, {...}]` when `$alt=json` parameter is present (used by LangChain)
- System instructions can be passed via `config.systemInstruction` or automatically from mission context
- When using LangChain's `ChatGoogleGenerativeAI`, specify `transport="rest"` to use the HTTP/REST protocol. The default gRPC transport is not supported by this endpoint.

---

# 4. Generate Summary API Endpoint

The Generate Summary API endpoint allows you to interact with the Matcha AI backend to generate a summary or pull out specific information from the provided input. This endpoint is useful for summarizing large amounts of text or extracting key information from documents.

Please note that this endpoint does not use any content from files that have been uploaded to Matcha. This is purely a text summarization endpoint that uses the LLM to analyze only the input text you provide.

The endpoint is as follows:

```
POST https://matcha.harriscomputer.com/rest/api/v1/generate_summary
```

The request body should be in JSON format.

Example using `curl` command for a simple summary request against the globally available "Call Center Summary API (example)" mission. This mission has been configured to try and give reason why a utility customer might be calling. The answer is based on a block of text of information about the customer from a CIS system:

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/v1/generate_summary \
  -H "Content-Type: application/json" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY" \
  -d '{
    "mission_id": "845",
    "input": "Customer status: inactive, Customer name: Tom, Balance: 102.34"
}'
```

**Response**
```
{
    "status": "success",
    "error": "",
    "output": [
        {
            "role": "assistant",
            "content": [
                {
                    "type": "output_text",
                    "text": "The customer is likely calling to inquire about or resolve an issue with their inactive account status."
                }
            ]
        }
    ]
}
```

## Full Summary Request Object Specification

**mission_id** (integer, required): The mission that will establish the settings and scope for the summarization llm to use.

**input** (string | json, required): A single string input that will be used as the prompt.

**context** (string, optional): Any additional context you want to provide the LLM besides
                      what already exists in the mission

---

# 5. Legacy API Documentation

Legacy API documentation is available [here](/integrationdev)
---

# 6. Matcha Objects

The Matcha Objects API allows you to interact with the Matcha AI backend to manage and manipulate various objects within the system.

By default the endpoints will return a list of objects that you specify in the request. You can also use query parameters to only return certain columns or filter the results based on specific criteria.

The following items are available to view in the API:

[Workspaces](#objects_section_workspaces)  
[Missions](#objects_section_missions)  
[Folders](#objects_section_folders)  
[Files](#objects_section_files)  
[Available LLMs](#objects_section_llms)

### Workspaces

```
GET https://matcha.harriscomputer.com/rest/api/v1/workspaces
```

Example using `curl` command for retrieving the list of workspaces you have access to:

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/v1/workspaces?select=id,name,workspace_type \
  -H "Accept: application/json" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY"
```

**Response**
```
[
    {
        "id": 1,
        "name": "My Business Unit",
        "workspace_type": "Business Unit"
    },
    {
        "id": 2,
        "name": "My Private Workspace",
        "workspace_type": "Personal"
    }
]
```

## Required Query Parameters

**** (, ): 

## Optional Query Parameters

**select** (list, optional): A comma-separated list of columns to return in the response. If not specified, all columns will be returned.  
  
*Possible values:*
                      
id
                      
name
                      
description
                      
workspace_type (Business Unit, Personal, Shared)

### Missions

```
GET https://matcha.harriscomputer.com/rest/api/v1/missions
```

Example using `curl` command for retrieving the list of missions you have access to:

**Request**
```
curl 'https://matcha.harriscomputer.com/rest/api/v1/missions?select=id,name,description&workspace_id=-1' \
  -H "Accept: application/json" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY"
```

**Response**
```
[
    {
        "id": 3,
        "name": "Example Mission",
        "description": "This is an example mission."
    },
    {
        "id": 4,
        "name": "Copied from All Harris",
        "description": "This mission was copied from all Harris."
    }
]
```

## Required Query Parameters

**workspace_id** (integer, required): The ID of the workspace to retrieve missions from. This is required to filter missions
                      by the workspace they belong to.

## Optional Query Parameters

**select** (list, optional): A comma-separated list of columns to return in the response. If not specified, all columns will be returned.  
  
*Possible values:*
  
id
  
workspace_id
  
workspace_name
  
name
  
description
  
prompts
  
syscontext
  
chat_settings
  
summaryprecontext
  
summarypostcontext
  
chatbot_blurb
  
updated_at
  
mission_blurb
  
folders_count
  
files_count
  
prompts_count
  
favorite

### Folders

```
GET https://matcha.harriscomputer.com/rest/api/v1/folders
```

Example using `curl` command for retrieving the list of folders you have access to:

**Request**
```
curl 'https://matcha.harriscomputer.com/rest/api/v1/folders?select=id,name,folder_blurb&mission_id=423' \
  -H "Accept: application/json" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY"
```

**Response**
```
[
    {
        "id": 5,
        "name": "Empty Folder",
        "files_count": 0
    },
    {
        "id": 6,
        "name": "Folder with Files",
        "files_count": 2
    }
]
```

## Required Query Parameters

**mission_id** (integer, required): The ID of the mission to retrieve folders from. This is required to filter folders
                      by the mission they belong to.

## Optional Query Parameters

**select** (list, optional): A comma-separated list of columns to return in the response. If not specified, all columns will be returned.  
  
*Possible values:*
  
id
  
name
  
workspace_id
  
workspace_name
  
mission_id
  
mission_name
  
folder_blurb
  
files_count
  
critical_items_summary

### Files

```
GET https://matcha.harriscomputer.com/rest/api/v1/files
```

Example using `curl` command for retrieving the list of files you have access to:

**Request**
```
curl 'https://matcha.harriscomputer.com/rest/api/v1/files?select=id,filename,status&folder_id=522' \
  -H "Accept: application/json" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY"
```

**Response**
```
[
  {
    "filename": "Microsoft Services Agreement.pdf",
    "id": 9945,
    "status": "Analyzed"
  }
]
```

## Required Query Parameters

**mission_id | folder_id | file_id** (integer, required): One of these three ids are required. This will determine which files are returned.

## Optional Query Parameters

**select** (list, optional): A comma-separated list of columns to return in the response. If not specified, all columns will be returned.  
  
*Possible values:*
  
id
  
mission_id
  
mission_name
  
folder_id
  
folder_name
  
workspace_id
  
workspace_name
  
filename
  
analysis
  
status
  
summary

### Available LLMs

```
GET https://matcha.harriscomputer.com/rest/api/v1/llms
```

Example using `curl` command for retrieving the list of LLMs you have access to:

**Request**
```
curl https://matcha.harriscomputer.com/rest/api/v1/llms?select=id,list_header,name,character_limit \
  -H "Accept: application/json" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY"
```

**Response**
```
[
    {
        "id": 29,
        "list_header": "Harris Computer",
        "name": "Harris Rapid LLM (Internal)",
        "character_limit": 12000
    },
    {
        "id": 33,
        "list_header": "Google",
        "name": "Gemini 2.5 Flash-Lite",
        "character_limit": 999999
    }
]
```

## Required Query Parameters

**** (, ): 

## Optional Query Parameters

**select** (list, optional): A comma-separated list of columns to return in the response. If not specified, all columns will be returned.  
  
*Possible values:*
  
id
  
name
  
list_header
  
allowed_file_input_mime_types
  
character_limit
  
max_output_tokens
  
websearch_enabled
  
reranking_enabled
  
summarization_enabled
  
reasoning_enabled

---

# 7. File Upload

You can upload files to Matcha using the file upload API

### Files

```
POST https://matcha.harriscomputer.com/rest/api/v1/filePOST https://matcha.harriscomputer.com/rest/api/v1/upload (legacy)
```

Upload a file to a folder. You can use either the path-based route (recommended) or the legacy route. Example using `curl` command for uploading a pdf file to a folder:

**Request (Recommended)**
```
curl -X POST -F "file=@/path/to/file.pdf" "https://matcha.harriscomputer.com/rest/api/v1/file?folder_id=$FOLDER_ID" \
  -H "Accept: application/json" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY"
```

**Response**
```
{
  "status": "success",
  "error": "",
  "output": [
    {
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "File 'file.pdf' uploaded successfully (9)!"
        }
      ]
    }
  ]
}
```

## Required Query Parameters

**folder_id** (integer, required): The ID of the folder to upload files to. Returns a 404 error with a clear message if the folder does not exist.

## Error Responses

**404 - Folder Not Found:**

        @keyframes fadeIn {
          from {
            opacity: 0;
            transform: translateY(10px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
      
```
{
  "status": "error",
  "error": "Folder with id '999' not found. Please provide a valid folder_id.",
  "output": []
}
```

**400 - Missing or Invalid folder_id:**

        @keyframes fadeIn {
          from {
            opacity: 0;
            transform: translateY(10px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
      
```
{
  "status": "error",
  "error": "Invalid or missing folder_id parameter. Please provide a valid folder_id.",
  "output": []
}
```

---

# 8. Folder Creation

You can create a folder inside a mission using the folder creation API.

### Create a folder

```
POST https://matcha.harriscomputer.com/rest/api/v1/folder
```

Example using `curl` command for creating an empty folder.

**Request**
```
curl --location 'https://matcha.harriscomputer.com/rest/api/v1/folder' \
--header "Accept: application/json" \
--header "MATCHA-API-KEY: $MATCHA_API_KEY" \
--data '{
    "name": "name of new folder",
    "mission_id": mission_id,
    "folder_instructions": "instructions for new folder"
}'
```

**Response**
```
{
    "status": "success",
    "error": "",
    "output": [
        {
            "role": "assistant",
            "content": [
                {
                    "type": "output_text",
                    "text": "Folder <folder name> uploaded successfully (<folder id>)!"
                }
            ]
        }
    ]
}
```

## Request Body Parameters

**name** (string, required): The name of the new folder to create.

**mission_id** (integer, required): The mission ID of the mission to create the folder in.

**folder_instructions** (integer, optional): The instructions for the new folder.

**is_icl_folder** (boolean, optional): Determines if files in this folder should be forced into the chatbot context (ICL stands for in-context learning). Only one ICL folder is allowed per Matcha mission. Defaults to false.

---

# 9. File CRUD Operations

The File CRUD API allows you to read, update, and delete files by ID. This enables you to manage file contents programmatically.

### Download/Read a File

```
GET https://matcha.harriscomputer.com/rest/api/v1/file/:file_idGET https://matcha.harriscomputer.com/rest/api/v1/download (legacy)
```

Download a file by its ID. The file will be returned as a binary download with the original filename. You can use either the path-based route (recommended) or the legacy query parameter route.

**Request (Path Parameter - Recommended)**
```
curl "https://matcha.harriscomputer.com/rest/api/v1/file/$FILE_ID" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY" \
  -o downloaded_file.pdf
```

**Response**
```
Binary file download
(Original file with Content-Type based on file extension)
```

## Request Path Parameters

**file_id** (integer, required): The ID of the file to download (for path-based route).

## Request Query Parameters

Only used with the legacy /download endpoint.

**file_id** (integer, required): The ID of the file to download (for legacy query parameter route).

### Update File Contents

```
PUT https://matcha.harriscomputer.com/rest/api/v1/file/:file_id
```

Update or overwrite a file's contents by ID. The file will be re-analyzed and re-indexed. The Vector DB is updated using a gap-free approach: new embeddings are added first, then old ones are removed, ensuring no knowledge gaps during the update process.

Note: The file must be sent as multipart/form-data (binary upload). The `-F` flag in curl automatically handles this.

**Request**
```
curl -X PUT "https://matcha.harriscomputer.com/rest/api/v1/file/$FILE_ID?folder_id=$FOLDER_ID" \
  -F "file=@/path/to/file.pdf" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY"
```

**Response**
```
{
  "status": "success",
  "error": "",
  "output": [
    {
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "File 'example.pdf' (id: $FILE_ID) updated successfully. File will be re-analyzed and Vector DB will be updated without knowledge gap (new added before old removed)."
        }
      ]
    }
  ]
}
```

## Request Path Parameters

**file_id** (integer, required): The ID of the file to update (specified in the URL path).

## Required Query Parameters

**folder_id** (integer, required): The ID of the folder that contains the file. This is required for verification to ensure the file belongs to the specified folder.

## Request Body (Multipart Form Data)

**file** (file (binary), required): The new file to upload. This will completely replace the existing file contents. Must be sent as multipart/form-data.

**File Requirements:**

- Maximum file size: 60 MB
- Allowed file types: PDF, DOC, DOCX, XLS, XLSX, PPTX, CSV, TXT, MD
- The original filename in the database will be preserved

Important: After updating a file, it will be automatically re-indexed. During this process, new embeddings are created and added to the Vector DB first, then old embeddings are removed. This ensures there's no gap where the file appears to have no content available for search.

### Delete a File

```
DELETE https://matcha.harriscomputer.com/rest/api/v1/file/:file_id
```

Delete a file by its ID. This will permanently remove the file and all associated data from Matcha.

**Request**
```
curl -X DELETE "https://matcha.harriscomputer.com/rest/api/v1/file/$FILE_ID?folder_id=$FOLDER_ID" \
  -H "MATCHA-API-KEY: $MATCHA_API_KEY"
```

**Response**
```
{
  "status": "success",
  "error": "",
  "output": [
    {
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "File 'example.pdf' (id: $FILE_ID) deleted successfully"
        }
      ]
    }
  ]
}
```

## Request Path Parameters

**file_id** (integer, required): The ID of the file to delete (specified in the URL path).

## Required Query Parameters

**folder_id** (integer, required): The ID of the folder that contains the file. This is required for verification to ensure the file belongs to the specified folder.

Warning: This operation is permanent and cannot be undone. All file contents, embeddings, and associated data will be permanently deleted.

# 10. File Summary

Enqueue a file to the file summary queue.

### Enqueue a file

```
POST https://matcha.harriscomputer.com/rest/api/v1/summarize_file
```

Example using `curl` command for enqueueing a file to the summary queue.

**Request**
```
curl --location 'https://matcha.harriscomputer.com/rest/api/v1/summarize_file' \
--header "Accept: application/json" \
--header "Content-Type: application/json" \
--header "matcha-api-key: $MATCHA_API_KEY" \
--data '{
    "file_id": 9945
}'
```

**Response**
```
{
  "status": "success",
  "error": "",
  "output": [
    {
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "file Microsoft Services Agreement.pdf summarizing started"
        }
      ]
    }
  ]
}
```

## Request Body Parameters

**file_id** (integer, required): The file ID of the file to summarize.

# 11. File/Folder Analysis

Enqueue a file or folder to the analysis queue.

### Enqueue a file to analysis queue

```
POST https://matcha.harriscomputer.com/rest/api/v1/analyze_file
```

Example using `curl` command for enqueueing a file to the analysis queue:

**Request**
```
curl --location 'https://matcha.harriscomputer.com/rest/api/v1/analyze_file' \
--header "Accept: application/json" \
--header "Content-Type: application/json" \
--header "matcha-api-key: $MATCHA_API_KEY" \
--data '{
    "file_id": 9945
}'
```

**Response**
```
{
  "status": "success",
  "error": "",
  "output": [
    {
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "file Microsoft Services Agreement.pdf analysis started"
        }
      ]
    }
  ]
}
```

## Request Body Parameters

**file_id** (integer, required): The file ID of the file to analyze.

### Enqueue a folder to analysis queue

```
POST https://matcha.harriscomputer.com/rest/api/v1/analyze_folder
```

Example using `curl` command for enqueiuing a folder to the analysis queue:

**Request**
```
curl --location 'https://matcha.harriscomputer.com/rest/api/v1/analyze_folder' \
--header "Accept: application/json" \
--header "Content-Type: application/json" \
--header "matcha-api-key: $MATCHA_API_KEY" \
--data '{
    "folder_id": 522
}'
```

**Response**
```
{
  "status": "success",
  "error": "",
  "output": [
    {
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "file Microsoft Services Agreement.pdf analysis started"
        }
      ]
    }
  ]
}
```

## Request Body Parameters

**folder_id** (integer, required): The folder ID of the folder to analyze.

# 12. Recompute file records

Recompute all file records, if any are configured.

### Recompute all file records

```
POST https://matcha.harriscomputer.com/rest/api/v1/recompute_file
```

Example using `curl` command for enqueueing a file to recompute computed columns queue.

**Request**
```
curl --location 'https://matcha.harriscomputer.com/rest/api/v1/recompute_file' \
--header "Accept: application/json" \
--header "Content-Type: application/json" \
--header "matcha-api-key: $MATCHA_API_KEY" \
--data '{
    "file_id": 9945
}'
```

**Response**
```
{
  "status": "success",
  "error": "",
  "output": [
    {
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "file records analysis started for file id 9945"
        }
      ]
    }
  ]
}
```

## Request Body Parameters

**file_id** (integer, required): The file ID of the file to recompute columns for.

