document.addEventListener("DOMContentLoaded", function() {
    document.getElementById("query-form").addEventListener("submit", function(event) {
        event.preventDefault();
        var query = document.getElementById("query-input").value;
        var loadingIndicator = document.getElementById("loading-indicator");

        // Show the loading indicator and change its style
        loadingIndicator.hidden = false;

        fetch("/query", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({ query: query })
        })
        .then(response => response.json())
        .then(data => {
            // Hide the loading indicator
            loadingIndicator.hidden = true;
            document.getElementById("results").innerHTML = data.table_html;

            // Move search box up
            document.getElementById("main-container").style.marginTop = "20px";
        })
        .catch(error => {
            console.error("Error:", error);
            // Hide the loading indicator even on error
            loadingIndicator.hidden = true;
        }); 
        /*setTimeout(function() {
            // Hide the loading indicator
            // Populate the table with test data
            var tableHTML = '<table border="1" class="dataframe">\n  <thead>\n    <tr style="text-align: right;">\n      <th>station_complex</th>\n      <th>station_complex_id</th>\n      <th>total_ridership</th>\n    </tr>\n  </thead>\n  <tbody>\n    <tr>\n      <td>Bedford Av</td>\n      <td>120</td>\n      <td>75494</td>\n    </tr>\n    <tr>\n      <td>Lorimer St /Metropolitan Av</td>\n      <td>629</td>\n      <td>53014</td>\n    </tr>\n    <tr>\n      <td>Atlantic Av-Barclays Ctr</td>\n      <td>617</td>\n      <td>46526</td>\n    </tr>\n  </tbody>\n</table>';
            document.getElementById("results").innerHTML = tableHTML;

            // Move search box up
            document.getElementById("main-container").style.marginTop = "20px";
        }, 3000); // 3000 milliseconds = 3 seconds */
    });
});
