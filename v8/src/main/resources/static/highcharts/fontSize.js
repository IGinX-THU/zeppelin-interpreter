<script>
        setTimeout(function(){
        if('OUTPUT_TYPE'==='TABLE'){
                document.querySelector('[id=\"PARAGRAPH_ID_container\"]').style.fontSize='FONT_SIZE';
        }else{
                var paragraphDiv = document.querySelector('[id=\"PARAGRAPH_ID_container\"]');
                var allChildren = paragraphDiv.querySelectorAll("*");
                Array.from(allChildren).forEach(function (child) {
                        if(matchesIdPattern(child.id)){
                                document.getElementById(child.id).style.setProperty('font-size', 'FONT_SIZE', 'important');
                        }
                });

        }}, 10);

        function matchesIdPattern(id) {
                const regex = /^pPARAGRAPH_ID_(\d+)_text$/;
                return regex.test(id);
        }
</script>