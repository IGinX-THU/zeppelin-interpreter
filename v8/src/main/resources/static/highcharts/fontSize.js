<script>
        setTimeout(function(){
                var paragraphDiv = document.querySelector('[id=\"PARAGRAPH_ID_container\"]');
                var allChildren = paragraphDiv.querySelectorAll("*");
                if('OUTPUT_TYPE'==='TABLE'){
                        document.querySelector('[id=\"PARAGRAPH_ID_container\"]').style.fontSize='FONT_SIZE'+'pt';
                } else{
                        Array.from(allChildren).forEach(function (child) {
                                if(matchesIdPattern(child.id,'text')){
                                        document.getElementById(child.id).style.setProperty('font-size', 'FONT_SIZE'+'pt', 'important');
                                }
                        });
                }
                // 命令区域字体
                document.querySelector('[id=\"PARAGRAPH_ID_editor\"]').style.fontSize='FONT_SIZE'+'pt';
                document.querySelector('[id=\"PARAGRAPH_ID_editor\"]').style.height=(FONT_SIZE*1.33 + 5)+'px';
                // 非table图表字体
                Array.from(allChildren).forEach(function (child) {
                        if(matchesIdPattern(child.id,'switch')){
                                var viewBtns =  document.getElementById(child.id).querySelectorAll("button");
                                Array.from(viewBtns).forEach(function (btn) {
                                        btn.addEventListener('click', function(){setTimeout(refreshView,100);});
                                        setTimeout(refreshView,100);
                                });
                        }
                });
        }, 10);

        function matchesIdPattern(id,part) {
                let regex;
                switch(part)
                {
                        case 'switch':
                                regex = /^PARAGRAPH_ID_(\d+)_switch$/;
                                break;
                        case 'text':
                                regex = /^pPARAGRAPH_ID_(\d+)_text$/;
                                break;
                        default:
                                console.log("not support element");
                }
                return regex.test(id);
        }

        function refreshView(event) {
                let allChildren = document.querySelectorAll('[id=\"PARAGRAPH_ID_container\"] .nvd3 text');
                Array.from(allChildren).forEach(function (child) {
                        child.style.font='400 FONT_SIZEpt Arial, sans-serif';
                });
        }
</script>
