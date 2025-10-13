$(document).ready(function() {
    initData();
});

function initData() {
    try {
        $.getJSON('/avg-rating-by-country', (data) => {
            $(data).map((i, obj) => {
                $('#avg-rating-by-country table tbody').append(`
                    <tr>
                        <td>${obj.country}</td>
                        <td>${obj.n_movies}</td>
                        <td>${obj.avg_rating}</td>
                    </tr>
                `);
            })
        })


        getRatingByCountry();
        getRatingByYear();
        getGenresByCountry();
        getMoviesByGenreAndYear();
        getReleasesByCountry();
    } catch (error) {
        console.log(error);
    }
}

function getRatingByCountry() {
    $('#avg-rating-by-country').click((e) => {
        const id = e.currentTarget.id.split('-tab')[0];
        const isEmpty = $('#avg-rating-by-country table tbody').is(':empty');
        if (!isEmpty) return;

        $.getJSON(`/${id}`, (data) => {
            $(data).map((i, obj) => {
                $('#avg-rating-by-country table tbody').append(`
                    <tr>
                        <td>${obj.country}</td>
                        <td>${obj.n_movies}</td>
                        <td>${obj.avg_rating}</td>
                    </tr>
                `);
            });
        })
    })
}

function getRatingByYear() {
    $('#avg-rating-by-year-tab').click((e) => {
        const id = e.currentTarget.id.split('-tab')[0];
        const isEmpty = $('#avg-rating-by-year table tbody').is(':empty');
        if (!isEmpty) return;

        $.getJSON(`/${id}`, (data) => {            
            $(data).map((i, obj) => {
                $('#avg-rating-by-year table tbody').append(`
                    <tr>
                        <td>${obj.avg_rating}</td>
                        <td>${obj.n_movies}</td>
                        <td>${obj.year}</td>
                    </tr>
                `);
            });
        })
    })
}

function getGenresByCountry() {
    $('#genres-by-country-tab').click((e) => {
        const id = e.currentTarget.id.split('-tab')[0];
        const isEmpty = $('#genres-by-country table tbody').is(':empty');
        if (!isEmpty) return;

        $.getJSON(`/${id}`, (data) => {
            $(data).map((i, obj) => {
                $('#genres-by-country table tbody').append(`
                    <tr>
                        <td>${obj.country}</td>
                        <td>${obj.genres.map((genre) => ` ${genre.genre} (${genre.count})`)}</td>
                    </tr>
                `);
            });
        })
    })
}

function getMoviesByGenreAndYear() {
    $('#movies-by-genre-and-year-tab').click((e) => {
        const id = e.currentTarget.id.split('-tab')[0];
        const isEmpty = $('#movies-by-genre-and-year table tbody').is(':empty');
        if (!isEmpty) return;

        $.getJSON(`/${id}`, (data) => {            
            $(data).map((i, obj) => {
                $('#movies-by-genre-and-year table tbody').append(`
                    <tr>
                        <td>${obj.genre}</td>
                        <td>${obj.n_movies}</td>
                        <td>${obj.year}</td>
                    </tr>
                `);
            });
        })
    })
}

function getReleasesByCountry() {
    $('#releases-by-country-tab').click((e) => {
        const id = e.currentTarget.id.split('-tab')[0];
        const isEmpty = $('#releases-by-country table tbody').is(':empty');
        if (!isEmpty) return;
        let html = '';

        $.getJSON(`/${id}`, (data) => {
            $(data).map((i, obj) => {
                html += '<tr>';
                html += '<td>' + obj.name + '</td>';
                html += '<td>';
                html += '<div class="countries">';

                $(obj.releases).map((i, release) => {
                    html += '<div class="country">';
                    html += '<strong>' + release.country + '</strong>';
                    if (release.releases && release.releases.length > 0) {
                        html += '<ul>';
                        $(release.releases).map((j, rel) => {
                            const date = new Date(rel.date_release).toLocaleDateString('pt-BR');
                            html += '<li>' + date + ' (' + rel.type + ')</li>';
                        });
                        html += '</ul>';
                    }

                    html += '</div>';
                });

                html += '</div>';
                html += '</td>';
                html += '</tr>';
            });

            $('#releases-by-country table tbody').html(html);
        });
    });
}