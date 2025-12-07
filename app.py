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

# --- SMART DATABASE CONNECTION ---
@st.cache_resource
def get_db():
    if not firebase_admin._apps:
        if "firebase" in st.secrets:
            try:
                key_dict = dict(st.secrets["firebase"])
                cred = credentials.Certificate(key_dict)
                firebase_admin.initialize_app(cred)
            except Exception as e:
                st.error(f"Error loading Cloud Secrets: {e}")
                st.stop()
        else:
            try:
                cred = credentials.Certificate("service_account.json")
                firebase_admin.initialize_app(cred)
            except Exception as e:
                st.error("No database key found! Check service_account.json")
                st.stop()
    return firestore.client()

db = get_db()

# --- HELPER: LOAD COLLECTION ---
def load_collection_to_df(collection_name):
    docs = db.collection(collection_name).stream()
    data = []
    for doc in docs:
        d = doc.to_dict()
        d['id'] = doc.id 
        data.append(d)
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)

# --- SIDEBAR ---
st.sidebar.title("Pelham Swimming")
menu_options = ["Home", "Enter Times (Batch)", "Edit/Fix Results", "Manage Swimmers", "Rankings", "Gala Reports"]
choice = st.sidebar.radio("Go to:", menu_options)

if "username" not in st.session_state:
    st.session_state["username"] = "Staff Member"

# --- PAGES ---

if choice == "Home":
    st.title("🏊 Pelham Interhouse Gala System")
    st.success(f"Connected to Cloud Database. Logged in as: {st.session_state['username']}")
    try:
        swimmer_count = len(list(db.collection('swimmers').stream()))
        st.metric("Total Swimmers", swimmer_count)
    except:
        st.metric("Status", "Connecting...")

elif choice == "Enter Times (Batch)":
    st.header("⏱️ Batch Time Entry")
    c1, c2, c3 = st.columns(3)
    grade_filter = c1.selectbox("Grade", [4, 5, 6, 7])
    stroke_filter = c2.selectbox("Stroke", ["Freestyle", "Breaststroke", "Backstroke", "Butterfly"])
    gender_filter = c3.selectbox("Gender", ["All", "M", "F"])
    
    swimmers_ref = db.collection('swimmers')
    query = swimmers_ref.where('grade', '==', grade_filter)
    if gender_filter != "All":
        query = query.where('gender', '==', gender_filter)
    docs = query.stream()
    
    data = []
    for doc in docs:
        s = doc.to_dict()
        data.append({
            "id": doc.id,
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
                "Time (Seconds)": st.column_config.NumberColumn("Time", min_value=0.0, format="%.2f"),
            },
            hide_index=True,
            use_container_width=True,
            num_rows="fixed"
        )

        if st.button("Submit Results"):
            count = 0
            current_year = datetime.now().year
            current_date_str = datetime.now().strftime("%Y-%m-%d")
            progress_bar = st.progress(0)
            
            for index, row in edited_df.iterrows():
                try:
                    raw_time = row['Time (Seconds)']
                    if isinstance(raw_time, list): time_val = float(raw_time[0])
                    else: time_val = float(raw_time)
                except: time_val = 0.0

                if time_val > 0 and not row['DNS']:
                    db.collection('results').add({
                        "swimmer_id": row['id'],
                        "stroke": stroke_filter,
                        "time_seconds": time_val,
                        "date_swum": current_date_str,
                        "season": current_year,
                        "source": "Trials",
                        "logged_by": st.session_state["username"],
                        "timestamp": firestore.SERVER_TIMESTAMP
                    })
                    count += 1
                progress_bar.progress((index + 1) / len(edited_df))

            st.success(f"✅ Saved {count} results!")
            st.balloons()
    else:
        st.warning("No swimmers found.")

# --- NEW SECTION: EDIT RESULTS ---
elif choice == "Edit/Fix Results":
    st.header("✏️ Edit or Fix Results")
    st.info("Select a swimmer to view and correct their history.")
    
    # 1. Search for a Swimmer
    all_swimmers = load_collection_to_df('swimmers')
    if not all_swimmers.empty:
        # Create a display label "John Smith (Gr 4)"
        all_swimmers['display'] = all_swimmers['first_name'] + " " + all_swimmers['surname'] + " (Gr " + all_swimmers['grade'].astype(str) + ")"
        
        selected_swimmer_name = st.selectbox("Search Swimmer", all_swimmers['display'].unique())
        
        # Get the ID of the selected swimmer
        swimmer_id = all_swimmers[all_swimmers['display'] == selected_swimmer_name].iloc[0]['id']
        
        # 2. Get their results
        results_ref = db.collection('results')
        # We fetch all results for this swimmer
        q = results_ref.where('swimmer_id', '==', swimmer_id).stream()
        
        res_data = []
        for doc in q:
            d = doc.to_dict()
            d['doc_id'] = doc.id # Valid Firestore ID
            res_data.append(d)
        
        df_history = pd.DataFrame(res_data)
        
        if not df_history.empty:
            st.subheader(f"History for {selected_swimmer_name}")
            
            # Show Data Editor
            # We allow editing Time, Date, and Stroke
            edited_history = st.data_editor(
                df_history,
                column_config={
                    "doc_id": st.column_config.TextColumn(disabled=True, help="System ID"),
                    "season": st.column_config.NumberColumn(disabled=True),
                    "source": st.column_config.TextColumn(disabled=True),
                    "logged_by": st.column_config.TextColumn(disabled=True),
                    "time_seconds": st.column_config.NumberColumn("Time (Sec)", min_value=0.0, format="%.2f"),
                    "date_swum": st.column_config.DateColumn("Date Swum"),
                    "stroke": st.column_config.SelectboxColumn("Stroke", options=["Freestyle", "Breaststroke", "Backstroke", "Butterfly"])
                },
                hide_index=True,
                use_container_width=True,
                num_rows="dynamic" # Allows deleting rows!
            )
            
            if st.button("Save Changes to History"):
                # Iterate through the edited dataframe and update Cloud
                progress = st.progress(0)
                updated_count = 0
                
                for index, row in edited_history.iterrows():
                    # We utilize the doc_id to find the record in the cloud
                    doc_ref = db.collection('results').document(row['doc_id'])
                    
                    # Update the fields
                    doc_ref.update({
                        "time_seconds": float(row['time_seconds']),
                        "date_swum": str(row['date_swum']), # Ensure string format YYYY-MM-DD
                        "stroke": row['stroke']
                    })
                    updated_count += 1
                    progress.progress((index + 1) / len(edited_history))
                
                st.success("History updated successfully!")
                
        else:
            st.warning("No results found for this swimmer.")
    else:
        st.warning("No swimmers in database.")

elif choice == "Manage Swimmers":
    st.header("👥 Manage Student Database")
    tab1, tab2 = st.tabs(["Add Single Swimmer", "Bulk Upload (CSV)"])

    with tab1:
        with st.form("add_swimmer_form"):
            c1, c2 = st.columns(2)
            f_name = c1.text_input("First Name")
            s_name = c2.text_input("Surname")
            c3, c4 = st.columns(2)
            dob = c3.date_input("Date of Birth", min_value=datetime(2010, 1, 1))
            gender = c4.selectbox("Gender", ["M", "F"])
            c5, c6 = st.columns(2)
            house = c5.selectbox("House", ["Bromhead", "Christie", "Clark", "Melville"])
            grade = c6.selectbox("Grade", [4, 5, 6, 7])
            
            if st.form_submit_button("Add Swimmer"):
                db.collection('swimmers').add({
                    "first_name": f_name, "surname": s_name, "dob": dob.strftime("%Y-%m-%d"),
                    "gender": gender, "grade": grade, "house": house, "active": True
                })
                st.success(f"Added {f_name} {s_name}")

    with tab2:
        st.subheader("Upload Class List")
        template_data = {'First Name': [], '