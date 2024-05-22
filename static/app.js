$(document).ready(function() {
    $("#query-form").submit(function(event) {
        event.preventDefault();
        var query = $("#query-input").val();
        
        $.ajax({
            url: "/query",
            type: "POST",
            contentType: "application/json",
            data: JSON.stringify({ query: query }),
            success: function(response) {
                $("#results").html(response.table_html);
            },
            error: function(error) {
                console.log("Error:", error);
            }
        });
    });
});
