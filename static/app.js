document.addEventListener("DOMContentLoaded", function() {
    document.getElementById("query-form").addEventListener("submit", function(event) {
        event.preventDefault();
        var query = document.getElementById("query-input").value;
        var loadingIndicator = document.getElementById("loading-indicator");
        var resultsSection = document.getElementById("results");
        var sqlContainer = document.getElementById("sql-container");
        var sqlSnippet = document.getElementById("sql-snippet")

        // Show the loading indicator and change its style
        resultsSection.hidden = true;
        sqlContainer.hidden = true;
        loadingIndicator.hidden = false;


        fetch("/query", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({ query: query })
        })
        .then(response => {
            if (!response.ok) {
                throw new Error("Network response was not ok.")
            }
            return response.json();
        })
        .then(data => {
            // Hide the loading indicator
            loadingIndicator.hidden = true;
            resultsSection.hidden = false;
            sqlContainer.hidden = false;
            sqlSnippet.textContent = data.query;
            resultsSection.innerHTML = data.table_html;

            saveQueryToLocalStorage(query, data.query)
        })
        .catch(error => {
            console.error("Error:", error);
            // Hide the loading indicator even on error
            loadingIndicator.hidden = true;
            resultsSection.innerHTML = '<p style="text-align: center;">There was an error processing your query.<br>Make sure the query is relevant to the data and try again.</p>'
            resultsSection.hidden = false;
        }); 
    });
});

function toggleSQL() {
    var sqlSnippet = document.getElementById("sql-snippet");
    var sqlBlock = document.getElementById("sql-block");
    var sqlActions = document.getElementById("sql-actions");
    var collapsible = document.getElementById("sql-trigger");

    if (sqlSnippet.style.display === "flex") {
        sqlSnippet.style.display = "none";
        sqlBlock.style.display = "none";
        sqlActions.style.display = "none";
        collapsible.classList.remove("open");
    } else {
        sqlSnippet.style.display = "flex";
        sqlBlock.style.display = "flex";
        sqlActions.style.display = "flex";
        collapsible.classList.add("open");
    }
}

function copySQL() {
    var sqlSnippet = document.getElementById("sql-snippet");
    var range = document.createRange();
    range.selectNode(sqlSnippet);
    window.getSelection().removeAllRanges(); // clear current selection
    window.getSelection().addRange(range); // to select text
    document.execCommand("copy");
    window.getSelection().removeAllRanges(); // to deselect
}

function saveQueryToLocalStorage(userQuery, sqlQuery) {
    let recentQueries = JSON.parse(localStorage.getItem("recentQueries")) || [];

    recentQueries.push({
        user_query: userQuery,
        sql_query: sqlQuery
    });

    if (recentQueries > 100) {
        recentQueries.shift();
    }
    localStorage.setItem("recentQueries", JSON.stringify(recentQueries));
}

