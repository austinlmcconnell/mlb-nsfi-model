import streamlit as st
import pandas as pd
import requests
from io import StringIO

# Title of Streamlit application
st.title('MLB First Inning Betting Dashboard')

# Define the URL of the CSV file
csv_url = 'https://drive.google.com/uc?id=1DWaPu--9WYLKMKoyHV5Yeac3xXS59mAK'

# Fetch the CSV file from Google Drive
response = requests.get(csv_url)
if response.status_code == 200:
    # Convert the response content to a StringIO object for pandas
    csv_content = StringIO(response.text)
    try:
        # Read the CSV into a DataFrame
        output_df = pd.read_csv(csv_content)
        
        # Ensure 'game_id' is the first column and set it as index
        if 'game_id' in output_df.columns:
            output_df = output_df.set_index('game_id')

            # Create a dictionary for easy lookup
            model_output_dict = output_df.T.to_dict('dict')

            # Custom sorting function
            def custom_sort(game_id):
                # Split the game_id into parts
                parts = game_id.split(' - ')
                if len(parts) == 2:
                    game, inning = parts
                    # Define sorting criteria
                    inning_order = {'Top': 1, 'Bot': 2}
                    # Return a tuple that sorts first by game and then by inning
                    # Convert inning to order
                    inning_sort_order = inning_order.get(inning, 3)
                    return (game, inning_sort_order)
                return (game_id, 3)  # Fallback for unexpected formats

            # Apply custom sorting to the game_ids
            sorted_game_ids = sorted(output_df.index.unique(), key=custom_sort)

            # Dropdown menu to select a game
            selected_game = st.selectbox("Select a Game:", sorted_game_ids)

            def color_code_expected_value(ev):
                if ev > 0.05:
                    color = 'green'
                elif ev >= 0:
                    color = 'yellow'
                else:
                    color = 'red'
                return color

            # Display the probabilities and expected value of bets for the selected game
            if selected_game:
                output = model_output_dict.get(selected_game, {})
                if output:
                    st.write(f"**Probability of No Strikeouts:** {output['probability_no_strikeouts']*100:.2f}%")
                    st.write(f"**Implied Probability from Betting Odds:** {output['implied_probability_no_strikeouts']*100:.2f}%")
                    ev_no_strikeouts = output['ev_no_strikeouts'] * 100
                    color = color_code_expected_value(output['ev_no_strikeouts'])
                    st.markdown(f"<p style='color: {color};'>**Expected Value:** {ev_no_strikeouts:.2f}%</p>", unsafe_allow_html=True)

                    st.write('')  # Adding a gap

                    st.write(f"**Probability of No Hits:** {output['probability_no_hits']*100:.2f}%")
                    st.write(f"**Implied Probability from Betting Odds:** {output['implied_probability_no_hits']*100:.2f}%")
                    ev_no_hits = output['ev_no_hits'] * 100
                    color = color_code_expected_value(output['ev_no_hits'])
                    st.markdown(f"<p style='color: {color};'>**Expected Value:** {ev_no_hits:.2f}%</p>", unsafe_allow_html=True)

                    st.write('')  # Adding a gap

                    st.write(f"**Probability of Less Than Four Batters:** {output['probability_under_four_batters_to_plate']*100:.2f}%")
                    st.write(f"**Implied Probability from Betting Odds:** {output['implied_probability_under_four_batters_to_plate']*100:.2f}%")
                    ev_under_four_batters_to_plate = output['ev_under_four_batters_to_plate'] * 100
                    color = color_code_expected_value(output['ev_under_four_batters_to_plate'])
                    st.markdown(f"<p style='color: {color};'>**Expected Value:** {ev_under_four_batters_to_plate:.2f}%</p>", unsafe_allow_html=True)

    except pd.errors.ParserError as e:
        st.write(f"CSV parsing error: {e}")
    except Exception as e:
        st.write(f"Error reading CSV file: {e}")

else:
    st.write(f"Failed to download file, status code: {response.status_code}")
