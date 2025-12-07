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
                            "first_name": row['First Name'], "surname": row['Surname'],
                            "dob": row['DOB'], "gender": row['Gender'],
                            "grade": int(row['Grade']), "house": row['House'], "active": True
                        })
                        count += 1
                    except Exception as e:
                        st.write(f"Error on row {index}: {e}")
                st.success(f"Imported {count} swimmers.")

elif choice == "Rankings":
    st.header("🏆 Interhouse Team Selection")
    
    col1, col2, col3, col4 = st.columns(4)
    r_grade = col1.selectbox("Grade", [4, 5, 6, 7])
    r_stroke = col2.selectbox("Stroke", ["Freestyle", "Breaststroke", "Backstroke", "Butterfly"])
    r_gender = col3.selectbox("Gender", ["M", "F"])
    
    # UPDATED RANKING METHOD
    calc_method = col4.radio("Method", ["Best Time", "Average of Last N", "Last Swim"])
    
    n_val = 3
    if calc_method == "Average of Last N":
        n_val = st.slider("N", 2, 5, 3)

    swimmers_ref = db.collection('swimmers')
    q_swimmers = swimmers_ref.where('grade', '==', r_grade).where('gender', '==', r_gender).stream()
    
    swimmer_list = []
    for doc in q_swimmers:
        d = doc.to_dict()
        d['swimmer_id'] = doc.id
        swimmer_list.append(d)
    df_swimmers = pd.DataFrame(swimmer_list)

    if not df_swimmers.empty:
        results_ref = db.collection('results')
        q_results = results_ref.where('stroke', '==', r_stroke).stream()
        
        result_list = []
        for doc in q_results:
            d = doc.to_dict()
            result_list.append(d)
        df_results = pd.DataFrame(result_list)
        
        if not df_results.empty:
            df_merged = pd.merge(df_swimmers, df_results, on="swimmer_id")
            
            if not df_merged.empty:
                ranked_data = []
                grouped = df_merged.groupby(['first_name', 'surname', 'house'])
                
                for name, group in grouped:
                    final_time = 0.0
                    note = ""
                    
                    # LOGIC FOR CALCULATIONS
                    if calc_method == "Best Time":
                        final_time = group['time_seconds'].min()
                        note = f"Best of {len(group)}"
                    
                    elif calc_method == "Last Swim":
                        # Sort by Date descending
                        group = group.sort_values(by='date_swum', ascending=False)
                        # Take the first one (most recent)
                        final_time = group.iloc[0]['time_seconds']
                        note = f"Date: {group.iloc[0]['date_swum']}"

                    else: # Average of Last N
                        group = group.sort_values(by='date_swum', ascending=False)
                        top_n = group.head(n_val)
                        final_time = top_n['time_seconds'].mean()
                        note = f"Avg of {len(top_n)}"

                    ranked_data.append({
                        "First Name": name[0],
                        "Surname": name[1],
                        "House": name[2],
                        "Rank Time": round(final_time, 2),
                        "Note": note
                    })
                
                df_rank = pd.DataFrame(ranked_data).sort_values("Rank Time").reset_index(drop=True)
                df_rank.index += 1
                
                st.dataframe(df_rank, use_container_width=True)
                
                st.divider()
                st.subheader("Top 3 Per House")
                houses = ["Bromhead", "Christie", "Clark", "Melville"]
                cols = st.columns(len(houses))
                for i, h in enumerate(houses):
                    with cols[i]:
                        st.markdown(f"**{h}**")
                        team = df_rank[df_rank['House'] == h].head(3)
                        if not team.empty:
                            st.table(team[['First Name', 'Surname', 'Rank Time']])
                        else:
                            st.caption("No qualifiers")
            else:
                st.warning("No results found.")
        else:
            st.warning("No results entered yet.")
    else:
        st.warning("No swimmers found.")

elif choice == "Gala Reports":
    st.header("🖨️ PDF Reports")
    st.info("Generates PDF based on Cloud Data.")
    if st.button("Generate PDF"):
        df_swimmers = load_collection_to_df('swimmers')
        df_results = load_collection_to_df('results')
        
        if not df_swimmers.empty and not df_results.empty:
            df_swimmers = df_swimmers.rename(columns={'id': 'swimmer_id'})
            df_full = pd.merge(df_swimmers, df_results, on="swimmer_id")
            
            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("Arial", "B", 16)
            pdf.cell(0, 10, "Pelham Senior Primary - Gala Report", ln=1, align="C")
            pdf.ln(10)
            
            houses = ["Bromhead", "Christie", "Clark", "Melville"]
            for house in houses:
                pdf.set_font("Arial", "B", 14)
                pdf.set_fill_color(200, 220, 255)
                pdf.cell(0, 10, f"TEAM: {house}", 1, 1, 'L', fill=True)
                pdf.ln(2)
                
                house_data = df_full[df_full['house'] == house]
                pdf.set_font("Arial", "", 10)
                for grade in [4, 5, 6, 7]:
                    for gender in ['F', 'M']:
                        for stroke in ["Freestyle", "Breaststroke", "Backstroke", "Butterfly"]:
                            race_data = house_data[
                                (house_data['grade'] == grade) &
                                (house_data['gender'] == gender) &
                                (house_data['stroke'] == stroke)
                            ]
                            if not race_data.empty:
                                race_data = race_data.sort_values("time_seconds").head(3)
                                names = ", ".join([f"{r['first_name']} {r['surname']} ({r['time_seconds']}s)" for i, r in race_data.iterrows()])
                                label = f"Gr{grade} {gender} {stroke}:"
                                pdf.set_font("Arial", "B", 10)
                                pdf.cell(50, 6, label, border=0)
                                pdf.set_font("Arial", "", 10)
                                pdf.multi_cell(0, 6, names, border=0)
                pdf.ln(5)

            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                pdf.output(tmp_file.name)
                with open(tmp_file.name, "rb") as file:
                    st.download_button("Download PDF", file.read(), "Pelham_Gala_Report.pdf")
        else:
            st.error("Not enough data.")