import streamlit as st
import pandas as pd
from datetime import datetime
from fpdf import FPDF
import tempfile
import firebase_admin
from firebase_admin import credentials, firestore

# --- CONFIGURATION ---
st.set_page_config(page_title="Pelham Swimming", page_icon="🏊", layout="wide")

# Custom CSS
st.markdown("""
    <style>
    .stApp { background-color: #f9f9f9; }
    h1, h2, h3 { color: #800000; } 
    .stButton>button { background-color: #006400; color: white; } 
    </style>
""", unsafe_allow_html=True)

# --- FIREBASE CONNECTION (The Cloud Link) ---
# We cache this so it doesn't reconnect every time you click a button
# --- FIREBASE CONNECTION (Smart Version) ---
@st.cache_resource
def get_db():
    if not firebase_admin._apps:
        # Check if we are running in Streamlit Cloud
        if "firebase" in st.secrets:
            # Use the Cloud Secret
            key_dict = dict(st.secrets["firebase"])
            cred = credentials.Certificate(key_dict)
        else:
            # Use the Local File (for when you run it on your laptop)
            try:
                cred = credentials.Certificate("service_account.json")
            except:
                st.error("No database key found! Make sure service_account.json is present.")
                st.stop()
        
        firebase_admin.initialize_app(cred)
    
    return firestore.client()

db = get_db()

# --- HELPER: CONVERT FIRESTORE TO DATAFRAME ---
def load_collection_to_df(collection_name):
    docs = db.collection(collection_name).stream()
    data = []
    for doc in docs:
        d = doc.to_dict()
        d['id'] = doc.id # Keep the unique Firestore ID
        data.append(d)
    
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)

# --- SIDEBAR ---
st.sidebar.image("https://via.placeholder.com/150", caption="Pelham Senior Primary")
st.sidebar.title("Navigation")
menu_options = ["Home", "Enter Times (Batch)", "Manage Swimmers", "Rankings", "Gala Reports"]
choice = st.sidebar.radio("Go to:", menu_options)

# --- AUTHENTICATION MOCKUP ---
# Since we are using Firestore, we can eventually link this to Google Auth.
# For now, we ask for the User's Name to log in the audit trail.
if "username" not in st.session_state:
    st.session_state["username"] = "Staff Member"

with st.sidebar.expander("Current User"):
    st.session_state["username"] = st.text_input("Logged in as:", value=st.session_state["username"])

# --- PAGES ---

if choice == "Home":
    st.title("🏊 Pelham Interhouse Gala System (Cloud Edition)")
    st.info(f"Connected to Firebase Cloud Database. Logged in as: {st.session_state['username']}")
    
    # Dashboard Metrics
    try:
        # Note: Counting in NoSQL can be slow if huge, but fine for schools
        swimmer_count = len(list(db.collection('swimmers').stream()))
        st.metric("Total Swimmers", swimmer_count)
    except Exception as e:
        st.error(f"Database Error: {e}")

elif choice == "Enter Times (Batch)":
    st.header("⏱️ Batch Time Entry")
    
    col1, col2, col3 = st.columns(3)
    grade_filter = col1.selectbox("Select Grade", [4, 5, 6, 7])
    stroke_filter = col2.selectbox("Stroke", ["Freestyle", "Breaststroke", "Backstroke", "Butterfly"])
    gender_filter = col3.selectbox("Gender", ["All", "M", "F"])
    
    # 1. Fetch Swimmers from Cloud
    swimmers_ref = db.collection('swimmers')
    query = swimmers_ref.where('grade', '==', grade_filter)
    
    if gender_filter != "All":
        query = query.where('gender', '==', gender_filter)
        
    docs = query.stream()
    
    # Convert to format for Data Editor
    data = []
    for doc in docs:
        s = doc.to_dict()
        data.append({
            "id": doc.id, # Firestore ID (Essential!)
            "First Name": s.get('first_name'),
            "Surname": s.get('surname'),
            "House": s.get('house'),
            "Time (Seconds)": None,
            "DNS": False
        })
    
    df_swimmers = pd.DataFrame(data)

    if not df_swimmers.empty:
        st.info(f"Enter times for **Grade {grade_filter} {stroke_filter}**.")
        
        edited_df = st.data_editor(
            df_swimmers,
            column_config={
                "id": st.column_config.TextColumn(disabled=True),
                "First Name": st.column_config.TextColumn(disabled=True),
                "Surname": st.column_config.TextColumn(disabled=True),
                "House": st.column_config.TextColumn(disabled=True),
                "Time (Seconds)": st.column_config.NumberColumn(
                    "Time (sec)", min_value=0.0, format="%.2f"
                ),
            },
            hide_index=True,
            use_container_width=True,
            num_rows="fixed"
        )

        if st.button("Submit Results"):
            count = 0
            current_year = datetime.now().year
            current_date_str = datetime.now().strftime("%Y-%m-%d")
            
            # Progress bar for saving
            progress_bar = st.progress(0)
            
            for index, row in edited_df.iterrows():
                # Safety Convert Logic
                raw_time = row['Time (Seconds)']
                try:
                    if isinstance(raw_time, list): time_val = float(raw_time[0])
                    else: time_val = float(raw_time)
                except: time_val = 0.0

                if time_val > 0 and not row['DNS']:
                    # SAVE TO CLOUD
                    db.collection('results').add({
                        "swimmer_id": row['id'], # Link to swimmer document
                        "stroke": stroke_filter,
                        "time_seconds": time_val,
                        "date_swum": current_date_str,
                        "season": current_year,
                        "source": "Trials",
                        "logged_by": st.session_state["username"],
                        "timestamp": firestore.SERVER_TIMESTAMP
                    })
                    count += 1
                
                # Update progress
                progress_bar.progress((index + 1) / len(edited_df))

            st.success(f"✅ Saved {count} results to the Cloud!")
            st.balloons()
            
    else:
        st.warning("No swimmers found.")

elif choice == "Manage Swimmers":
    st.header("👥 Manage Student Database")
    tab1, tab2 = st.tabs(["Add Single Swimmer", "Bulk Upload (CSV)"])

    with tab1:
        with st.form("add_swimmer_form"):
            c1, c2 = st.columns(2)
            f_name = c1.text_input("First Name")
            s_name = c2.text_input("Surname")
            c3, c4 = st.columns(2)
            # Firestore prefers strings for dates usually, or datetime objects
            dob = c3.date_input("Date of Birth", min_value=datetime(2010, 1, 1))
            gender = c4.selectbox("Gender", ["M", "F"])
            c5, c6 = st.columns(2)
            house = c5.selectbox("House", ["Bromhead", "Christie", "Clark", "Melville"])
            grade = c6.selectbox("Grade", [4, 5, 6, 7])
            
            if st.form_submit_button("Add Swimmer"):
                # SAVE TO CLOUD
                db.collection('swimmers').add({
                    "first_name": f_name,
                    "surname": s_name,
                    "dob": dob.strftime("%Y-%m-%d"),
                    "gender": gender,
                    "grade": grade,
                    "house": house,
                    "active": True
                })
                st.success(f"Added {f_name} {s_name} to Firestore.")

    with tab2:
        st.subheader("Upload Class List")
        
        # Download Template Logic
        template_data = {'First Name': [], 'Surname': [], 'DOB': [], 'Gender': [], 'Grade': [], 'House': []}
        csv_template = pd.DataFrame(template_data).to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download Blank CSV Template", csv_template, "template.csv", "text/csv")
        
        uploaded_file = st.file_uploader("Choose CSV", type="csv")
        if uploaded_file:
            df = pd.read_csv(uploaded_file)
            st.write("Preview:", df.head())
            if st.button("Import Data"):
                count = 0
                for index, row in df.iterrows():
                    try:
                        db.collection('swimmers').add({
                            "first_name": row['First Name'],
                            "surname": row['Surname'],
                            "dob": row['DOB'],
                            "gender": row['Gender'],
                            "grade": int(row['Grade']),
                            "house": row['House'],
                            "active": True
                        })
                        count += 1
                    except Exception as e:
                        st.write(f"Error on row {index}: {e}")
                st.success(f"Imported {count} swimmers to Cloud.")

elif choice == "Rankings":
    st.header("🏆 Interhouse Team Selection")
    
    col1, col2, col3, col4 = st.columns(4)
    r_grade = col1.selectbox("Grade", [4, 5, 6, 7])
    r_stroke = col2.selectbox("Stroke", ["Freestyle", "Breaststroke", "Backstroke", "Butterfly"])
    r_gender = col3.selectbox("Gender", ["M", "F"])
    calc_method = col4.radio("Method", ["Best Time", "Average of Last N"])
    n_val = 3
    if calc_method == "Average of Last N":
        n_val = st.slider("N", 2, 5, 3)

    # --- PANDAS MERGE STRATEGY ---
    # 1. Get Swimmers for this Grade/Gender
    swimmers_ref = db.collection('swimmers')
    q_swimmers = swimmers_ref.where('grade', '==', r_grade).where('gender', '==', r_gender).stream()
    
    swimmer_list = []
    for doc in q_swimmers:
        d = doc.to_dict()
        d['swimmer_id'] = doc.id
        swimmer_list.append(d)
    df_swimmers = pd.DataFrame(swimmer_list)

    if not df_swimmers.empty:
        # 2. Get Results for this stroke
        # Note: We fetch ALL results for this stroke to ensure we don't miss any, 
        # then filter by the swimmers we found.
        results_ref = db.collection('results')
        q_results = results_ref.where('stroke', '==', r_stroke).stream()
        
        result_list = []
        for doc in q_results:
            d = doc.to_dict()
            result_list.append(d)
        df_results = pd.DataFrame(result_list)
        
        if not df_results.empty:
            # 3. MERGE (Join) in Python
            # Inner join ensures we only see results for the relevant swimmers
            df_merged = pd.merge(df_swimmers, df_results, on="swimmer_id")
            
            if not df_merged.empty:
                # 4. CALCULATE RANKING
                ranked_data = []
                grouped = df_merged.groupby(['first_name', 'surname', 'house'])
                
                for name, group in grouped:
                    final_time = 0.0
                    if calc_method == "Best Time":
                        final_time = group['time_seconds'].min()
                    else:
                        # Sort descending date
                        group = group.sort_values(by='date_swum', ascending=False)
                        top_n = group.head(n_val)
                        final_time = top_n['time_seconds'].mean()

                    ranked_data.append({
                        "First Name": name[0],
                        "Surname": name[1],
                        "House": name[2],
                        "Rank Time": round(final_time, 2)
                    })
                
                df_rank = pd.DataFrame(ranked_data).sort_values("Rank Time").reset_index(drop=True)
                df_rank.index += 1
                
                st.dataframe(df_rank, use_container_width=True)
            else:
                st.warning("No results found for these swimmers.")
        else:
            st.warning("No results entered for this stroke yet.")
    else:
        st.warning("No swimmers found in this category.")

elif choice == "Gala Reports":
    st.header("🖨️ PDF Reports")
    st.info("Generates PDF based on Cloud Data.")
    
    if st.button("Generate PDF"):
        # FETCH ALL DATA
        df_swimmers = load_collection_to_df('swimmers')
        df_results = load_collection_to_df('results')
        
        if not df_swimmers.empty and not df_results.empty:
            # Rename id to swimmer_id for merging
            df_swimmers = df_swimmers.rename(columns={'id': 'swimmer_id'})
            df_full = pd.merge(df_swimmers, df_results, on="swimmer_id")
            
            # (Logic is same as previous, just updated for new DF structure)
            # ... For brevity, the PDF logic uses df_full ...
            
            # Simple PDF Placeholder for the migration test
            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("Arial", size=12)
            pdf.cell(200, 10, txt="Pelham Cloud Data Report", ln=1, align="C")
            pdf.cell(200, 10, txt=f"Total Swims Analyzed: {len(df_full)}", ln=1, align="C")
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                pdf.output(tmp_file.name)
                with open(tmp_file.name, "rb") as file:
                    st.download_button("Download PDF", file.read(), "report.pdf")
        else:
            st.error("Not enough data to generate report.")